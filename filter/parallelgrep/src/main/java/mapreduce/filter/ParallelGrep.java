package mapreduce.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;


public class ParallelGrep
{
    private static final Logger LOGGER = Logger.getLogger(ParallelGrep.class);
    public static void main( String[] args ) throws IOException
    {
        int fileCount = Integer.parseInt(args[0]);
        String inputPath = args[0];
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(inputPath), configuration);
        fs.delete(new Path(args[1]), true);
        LOGGER.info(String.format("Generating input data in %s", inputPath));
    }
}
