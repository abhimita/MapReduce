package mapreduce.utility;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.io.FilenameUtils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.Arrays;
import java.util.List;

public class MapReduceUtility {
    public static String getS3BucketName(String s3FullPath) {
        String[] outputPathList = s3FullPath.split(File.separator);
        return outputPathList[2];
    }
    public static String getS3BucketKey(String s3FullPath) {
        String[] outputPathList = s3FullPath.split(File.separator);
        return String.join("/", Arrays.copyOfRange(outputPathList, 3, outputPathList.length));
    }
    public static void deleteS3Directory(String s3FullPath) {
        String bucketName = getS3BucketName(s3FullPath);
        String bucketKey = getS3BucketKey(s3FullPath);
        final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        ListObjectsV2Result result = s3.listObjectsV2(bucketName, bucketKey);
        List<S3ObjectSummary> objects = result.getObjectSummaries();
        for (S3ObjectSummary os : objects) {
            s3.deleteObject(bucketName, os.getKey());
        }
    }
    public static void writeToS3Directory(String s3FullPath, List<String> contents) {
        String bucketName = getS3BucketName(s3FullPath);
        String bucketKey = getS3BucketKey(s3FullPath);
        //deleteS3Directory("s3://" + bucketName + File.separator + FilenameUtils.getPathNoEndSeparator(bucketKey));
        final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType("application/text");
        byte[] fileContentInBytes = String.join("\n", contents).getBytes();
        metadata.setContentLength(fileContentInBytes.length);
        s3.putObject(bucketName, bucketKey, new ByteArrayInputStream(fileContentInBytes), metadata);
    }
}
