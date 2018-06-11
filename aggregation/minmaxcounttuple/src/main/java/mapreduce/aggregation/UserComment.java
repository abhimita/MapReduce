package mapreduce.aggregation;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserComment implements Writable {
    private String userName;
    private String commentDateTime;
    private String comment;

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getCommentDateTime() {
        return commentDateTime;
    }

    public void setCommentDateTime(String commentDateTime) {
        this.commentDateTime = commentDateTime;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        userName = in.readUTF();
        commentDateTime = in.readUTF();
        comment = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(userName);
        out.writeUTF(commentDateTime);
        out.writeUTF(comment);
    }
}
