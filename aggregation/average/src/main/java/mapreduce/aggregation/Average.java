package mapreduce.aggregation;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Average implements Writable {
    private String commentDate;
    private Integer commentCount;
    private Double averageCommentLength;

    public String getCommentDate() {
        return commentDate;
    }

    public void setCommentDate(String commentDate) {
        this.commentDate = commentDate;
    }

    public Integer getCommentCount() {
        return commentCount;
    }

    public void setCommentCount(Integer commentCount) {
        this.commentCount = commentCount;
    }

    public Double getAverageCommentLength() {
        return averageCommentLength;
    }

    public void setAverageCommentLength(Double averageCommentLength) {
        this.averageCommentLength = averageCommentLength;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        commentDate = in.readUTF();
        commentCount = in.readInt();
        averageCommentLength = in.readDouble();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(commentDate);
        out.writeInt(commentCount);
        out.writeDouble(averageCommentLength);
    }
}
