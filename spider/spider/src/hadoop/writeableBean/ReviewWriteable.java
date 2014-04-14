package hadoop.writeableBean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.io.WritableComparable;

public class ReviewWriteable implements WritableComparable<ReviewWriteable> {
	private String id;
	private String movieId;
	private String userId;
	private String title;
	private String detail;
	private String at;
	private Date date;
	private int useful;
	private int unUseful;
	private int start;
	private int replyNum;
	
	private List<ReplyWriteable> replys = new ArrayList<ReplyWriteable>();

	
	public List<ReplyWriteable> getReplys() {
		return replys;
	}

	public void setReplys(List<ReplyWriteable> replys) {
		this.replys = replys;
	}

	public int getStart() {
		return start;
	}

	public void setStart(int start) {
		this.start = start;
	}

	public int getReplyNum() {
		return replyNum;
	}

	public void setReplyNum(int replyNum) {
		this.replyNum = replyNum;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getMovieId() {
		return movieId;
	}

	public void setMovieId(String movieId) {
		this.movieId = movieId;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getDetail() {
		return detail;
	}

	public void setDetail(String detail) {
		this.detail = detail;
	}

	public String getAt() {
		return at;
	}

	public void setAt(String at) {
		this.at = at;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public int getUseful() {
		return useful;
	}

	public void setUseful(int useful) {
		this.useful = useful;
	}

	public int getUnUseful() {
		return unUseful;
	}

	public void setUnUseful(int unUseful) {
		this.unUseful = unUseful;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int compareTo(ReviewWriteable o) {
		// TODO Auto-generated method stub
		return 0;
	}

}
