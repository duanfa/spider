package parser.bean;

import java.util.List;


public class Movie {
	public enum MovieType{
		MOVIE,TV
	}
	private String id;
	private String name;
	private String img;
	private List<String> relativeMovies;
	private List<String> tags;
	
	private List<String> alies;
	private List<String> directors;
	private List<String> writers; 
	private List<String> actors;
	 
	private List<String> language;
	private List<String> showTime;
	private List<String> category;
	
	private  String produceCounty;
	private  String IMDb;
	private String detail;
	private  int timLong;
	private int startAll;
	private int start1;	
	private int start2;
	private int start3;
	private int start4;
	private int start5;
	
	private float startValue;
}
