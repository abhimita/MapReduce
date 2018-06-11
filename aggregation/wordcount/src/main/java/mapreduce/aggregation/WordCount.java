package mapreduce.aggregation;

import mapreduce.utility.MapReduceUtility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import java.io.IOException;

/**
 * Word count
 */
public class WordCount {
    private static final Logger logger = Logger.getLogger(WordCount.class);
    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            for (String s : value.toString().split("\\s+")) {
                word.set(s);
                context.write(word, new IntWritable(1));
            }
        }
    }

    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(configuration);
        Job job = Job.getInstance(configuration);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setJobName("WordCount");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        Path outputPath = new Path(args[1]);
        logger.info("Starting map reduce step");
        if (!args[1].startsWith("s3://") && fs.exists(outputPath)) {
            logger.info("Directory exists. Removing the directory");
            fs.delete(outputPath, true);
        } else {
            if (args[1].startsWith("s3://")) {
                MapReduceUtility.deleteS3Directory(args[1]);
            }
        }
        boolean status = job.waitForCompletion(true);
        if (status) {
            logger.info("Map reduce job completes successfully");
        } else {
            logger.info("Map reduce job has aborted");
        }
    }
}

