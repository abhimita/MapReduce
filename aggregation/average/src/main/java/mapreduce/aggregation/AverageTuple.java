package mapreduce.aggregation;

import mapreduce.utility.MinMaxCountTupleDataGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.time.format.DateTimeFormatter;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import org.json.JSONObject;

public class AverageTuple
{
    private static final Logger LOGGER = Logger.getLogger(AverageTuple.class);
    private static final DateTimeFormatter YEAR_MONTH_DAY_HOUR_MIN_SEC = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final NullWritable NULLWRITABLE = NullWritable.get();
    public static class AverageTupleMapper extends Mapper<LongWritable, Text, Text, Average> {
        private Average average = new Average();
        private Text dateKey = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\u0001");
            average.setCommentDate(fields[1]);
            average.setAverageCommentLength(new Double(fields[2].length()));
            average.setCommentCount(1);
            dateKey.set(fields[1].substring(0,10));
            context.write(dateKey, average);
        }
    }

    public static class AverageTupleCombiner extends Reducer<Text, Iterable<Average>, Text, Average> {
        private Average average = new Average();
        public void reduce(Text key, Iterable<Average> values, Context context) throws IOException, InterruptedException {
            Double s = 0.0;
            Integer n = 0;
            for (Average value: values) {
                s += value.getAverageCommentLength();
                n += value.getCommentCount();
            }
            average.setCommentDate(key.toString());
            average.setAverageCommentLength(s/n);
            average.setCommentCount(n);
            context.write(key, average);
        }
    }
    public static class AverageTupleReducer extends Reducer<Text, Average, Text, NullWritable> {
        private Text json = new Text();
        @Override
        public void reduce(Text key, Iterable<Average> values, Context context) throws IOException, InterruptedException {
            Double s = 0.0;
            Integer n = 0;
            for (Average value: values) {
                s += value.getAverageCommentLength();
                n += value.getCommentCount();
            }
            json.set(new JSONObject().put("commentDate", key).put("averageCommentLength", s/n).put("commentCount", n).toString());
            context.write(json, NULLWRITABLE);
        }
    }
    public static void main(String[] args) throws Exception {
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
        job.setJarByClass(AverageTuple.class);
        job.setMapperClass(AverageTupleMapper.class);
        job.setReducerClass(AverageTupleReducer.class);
        job.setCombinerClass(AverageTupleCombiner.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Average.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(1);
        LOGGER.info("Starting mapreduce step");
        boolean status = job.waitForCompletion(true);
        if (status) {
            LOGGER.info("Map reduce job completes successfully");
        } else {
            LOGGER.info("Map reduce job has aborted");
        }
    }
}
