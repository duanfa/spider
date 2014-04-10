package parser.bean;

import java.util.List;


public class Movie {
	public enum MovieType{
		MOVIE,TV
	}
	private String id;
	private String name;
	private String img;
	private String url;
	private List<String> relativeMovies;
	private List<String> relativeDouList;
	private List<String> relativeGroup;
	private List<String> userSeening;
	private List<String> tags;
	
	private List<String> alies;
	private List<String> directors;
	private List<String> writers; 
	private List<String> actors;
	 
	private List<String> language;
	private List<String> showTime;
	private List<String> category;
	private List<Review> reviews;
	
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

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getImg() {
		return img;
	}

	public void setImg(String img) {
		this.img = img;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public List<String> getRelativeMovies() {
		return relativeMovies;
	}

	public void setRelativeMovies(List<String> relativeMovies) {
		this.relativeMovies = relativeMovies;
	}

	public List<String> getRelativeDouList() {
		return relativeDouList;
	}

	public List<Review> getReviews() {
		return reviews;
	}

	public void setReviews(List<Review> reviews) {
		this.reviews = reviews;
	}

	public void setRelativeDouList(List<String> relativeDouList) {
		this.relativeDouList = relativeDouList;
	}

	public List<String> getRelativeGroup() {
		return relativeGroup;
	}

	public void setRelativeGroup(List<String> relativeGroup) {
		this.relativeGroup = relativeGroup;
	}

	public List<String> getUserSeening() {
		return userSeening;
	}

	public void setUserSeening(List<String> userSeening) {
		this.userSeening = userSeening;
	}

	public List<String> getTags() {
		return tags;
	}

	public void setTags(List<String> tags) {
		this.tags = tags;
	}

	public List<String> getAlies() {
		return alies;
	}

	public void setAlies(List<String> alies) {
		this.alies = alies;
	}

	public List<String> getDirectors() {
		return directors;
	}

	public void setDirectors(List<String> directors) {
		this.directors = directors;
	}

	public List<String> getWriters() {
		return writers;
	}

	public void setWriters(List<String> writers) {
		this.writers = writers;
	}

	public List<String> getActors() {
		return actors;
	}

	public void setActors(List<String> actors) {
		this.actors = actors;
	}

	public List<String> getLanguage() {
		return language;
	}

	public void setLanguage(List<String> language) {
		this.language = language;
	}

	public List<String> getShowTime() {
		return showTime;
	}

	public void setShowTime(List<String> showTime) {
		this.showTime = showTime;
	}

	public List<String> getCategory() {
		return category;
	}

	public void setCategory(List<String> category) {
		this.category = category;
	}

	public String getProduceCounty() {
		return produceCounty;
	}

	public void setProduceCounty(String produceCounty) {
		this.produceCounty = produceCounty;
	}

	public String getIMDb() {
		return IMDb;
	}

	public void setIMDb(String iMDb) {
		IMDb = iMDb;
	}

	public String getDetail() {
		return detail;
	}

	public void setDetail(String detail) {
		this.detail = detail;
	}

	public int getTimLong() {
		return timLong;
	}

	public void setTimLong(int timLong) {
		this.timLong = timLong;
	}

	public int getStartAll() {
		return startAll;
	}

	public void setStartAll(int startAll) {
		this.startAll = startAll;
	}

	public int getStart1() {
		return start1;
	}

	public void setStart1(int start1) {
		this.start1 = start1;
	}

	public int getStart2() {
		return start2;
	}

	public void setStart2(int start2) {
		this.start2 = start2;
	}

	public int getStart3() {
		return start3;
	}

	public void setStart3(int start3) {
		this.start3 = start3;
	}

	public int getStart4() {
		return start4;
	}

	public void setStart4(int start4) {
		this.start4 = start4;
	}

	public int getStart5() {
		return start5;
	}

	public void setStart5(int start5) {
		this.start5 = start5;
	}

	public float getStartValue() {
		return startValue;
	}

	public void setStartValue(float startValue) {
		this.startValue = startValue;
	}
	
	
}
