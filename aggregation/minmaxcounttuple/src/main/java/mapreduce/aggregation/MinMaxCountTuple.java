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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


// Finds first date&last date in which each user posted comments and number of comments
// Input file is ctrl-A delimited text file containing 3 fields:
// user name<string>,date time<string>,comment<string>import java.io.IOException;

public class MinMaxCountTuple {
    private static final Logger LOGGER = Logger.getLogger(MinMaxCountTuple.class);
    private static final DateTimeFormatter YEAR_MONTH_DAY_HOUR_MIN_SEC = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final NullWritable NULLWRITABLE = NullWritable.get();

    public static class MinMaxCountTupleMapper extends Mapper<LongWritable, Text, Text, UserComment> {
        private Text userName = new Text();
        private UserComment userComment = new UserComment();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\u0001");
            userComment.setUserName(fields[0]);
            userComment.setCommentDateTime(fields[1]);
            userComment.setComment(fields[2]);
            userName.set(fields[0]);
            context.write(userName, userComment);
        }
    }

    public static class MinMaxCountTupleReducer extends Reducer<Text, UserComment, Text, NullWritable> {

        public void reduce(Text key, Iterable<UserComment> values, Context context) throws IOException, InterruptedException {
            String maxCommentDateTime = "1900-01-01 00:00:00";
            String minCommentDateTime = "2050-01-01 00:00:00";
            int count = 0;
            Text json = new Text();

            for (UserComment value : values) {
                if (LocalDateTime.parse(value.getCommentDateTime(), YEAR_MONTH_DAY_HOUR_MIN_SEC).isAfter(LocalDateTime.parse(maxCommentDateTime, YEAR_MONTH_DAY_HOUR_MIN_SEC))) {
                    maxCommentDateTime = value.getCommentDateTime();
                }
                if (LocalDateTime.parse(value.getCommentDateTime(), YEAR_MONTH_DAY_HOUR_MIN_SEC).isBefore(LocalDateTime.parse(minCommentDateTime, YEAR_MONTH_DAY_HOUR_MIN_SEC))) {
                    minCommentDateTime = value.getCommentDateTime();
                }
                count++;
            }
            json.set(new JSONObject().put("userName", key).put("minCommentDateTime", minCommentDateTime).put("maxCommentDate", maxCommentDateTime).put("totalCommentCount", count).toString());
            context.write(json, NULLWRITABLE);
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
        job.setJarByClass(MinMaxCountTuple.class);
        job.setMapperClass(MinMaxCountTupleMapper.class);
        job.setReducerClass(MinMaxCountTupleReducer.class);
        //job.setCombinerClass(MinMaxCountTupleReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(UserComment.class);
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
