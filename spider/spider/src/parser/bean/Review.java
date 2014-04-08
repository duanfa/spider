package parser.bean;

import java.util.Date;



public class Review {
	public enum ReviewType{
		LONG,SHORT
	}
	private String id;
	private String reviewId;
	private String userId;
	private String title;
	private String detail;
	private String at;
	private Date date;
	private int useful;
	private int unUseful;
}
