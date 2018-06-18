package mapreduce.aggregation;

import mapreduce.utility.MinMaxCountTupleDataGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.Map;
import java.util.TreeMap;

public class MedianProcessor {
    private static final IntWritable ONE = new IntWritable(1);
    private static final Logger LOGGER = Logger.getLogger(MedianProcessor.class);
    public static class MedianProcessorMapper extends Mapper<LongWritable, Text, Text, SortedMapWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\u0001");
            SortedMapWritable outCommentLength = new SortedMapWritable();
            outCommentLength.put(new IntWritable(value.toString().length()), ONE);
            context.write(new Text(fields[1].substring(0, 10)), outCommentLength);
        }
    }
    public static class MedianProcessorReducer extends Reducer<Text, SortedMapWritable, Text, FloatWritable> {
        @Override
        public void reduce(Text key, Iterable<SortedMapWritable> values, Context context) throws IOException, InterruptedException {
            Map<Integer, Integer> commentLengthCounts = new TreeMap<>();
            for (SortedMapWritable v: values) {
                for (Map.Entry<WritableComparable, Writable> entry: v.entrySet()) {
                    int length = ((IntWritable)entry.getKey()).get();
                    int count = ((IntWritable)entry.getValue()).get();
                    if (commentLengthCounts.get(length) != null) {
                        commentLengthCounts.put(length, count + commentLengthCounts.get(length));
                    } else {
                        commentLengthCounts.put(length, count);
                    }
                }
                // Necessary because of https://issues.apache.org/jira/browse/HADOOP-5454
                // More details in https://stackoverflow.com/questions/9916549/hadoop-reduce-concatenating-current-value-with-previous-values
                v.clear();
            }
            int totalComments = commentLengthCounts.values().stream().mapToInt(i -> i).sum();
            int medianIndex = totalComments / 2;
            int previousCommentLength = 0;
            int previousCommentCount = 0;
            int commentCount;
            float result = 0.0f;
            for (Map.Entry<Integer, Integer> entry: commentLengthCounts.entrySet()) {
                commentCount = previousCommentCount + entry.getValue();
                if (previousCommentCount <= medianIndex && medianIndex < commentCount) {
                    if (totalComments % 2 == 0 && previousCommentCount == medianIndex) {
                        result = (entry.getKey() + previousCommentLength) / 2.0f;
                    } else {
                        result = entry.getKey();
                    }
                    break;
                }
                previousCommentCount = entry.getValue();
                previousCommentLength = entry.getKey();
            }
            context.write(key, new FloatWritable(result));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String inputPath = args[0];
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(inputPath), configuration);
        fs.delete(new Path(args[1]), true);
        LOGGER.info(String.format("Generating input data in %s", inputPath));
        try (
                FSDataOutputStream fsDataOutputStream = fs.create(new Path(inputPath + "//data.txt"));
                PrintWriter writer = new PrintWriter(fsDataOutputStream)
        ) {
            for (String line : MinMaxCountTupleDataGenerator.generate()) {
                writer.write(line + "\n");
            }
        }
        Job job = Job.getInstance(configuration);
        job.setJarByClass(MedianProcessor.class);
        job.setMapperClass(MedianProcessorMapper.class);
        job.setReducerClass(MedianProcessorReducer.class);
        //job.setCombinerClass(MinMaxCountTupleReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SortedMapWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputValueClass(FloatWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //job.setNumReduceTasks(1);
        LOGGER.info("Starting mapreduce step");
        boolean status = job.waitForCompletion(true);
        if (status) {
            LOGGER.info("Map reduce job completes successfully");
        } else {
            LOGGER.info("Map reduce job has aborted");
        }
    }
}
