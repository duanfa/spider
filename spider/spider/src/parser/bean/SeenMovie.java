package parser.bean;

import java.util.Date;

public class SeenMovie {
	public enum SeenType {
		DO, DID, WANT
	}

	private String userId;
	private String movieId;
	private Date date;
	private SeenType type;

	public SeenType getType() {
		return type;
	}

	public void setType(SeenType type) {
		this.type = type;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getMovieId() {
		return movieId;
	}

	public void setMovieId(String movieId) {
		this.movieId = movieId;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

}
