package parser.bean;

import java.util.Set;

public class User {
	private String id;
	private String name;
	private String sign;
	private Set<String> relativeUsers;
	private Set<String> recommendMovies;

	private String detail;

	private Set<String> seen;
	private Set<String> seening;
	private Set<String> wantSeens;
	private Set<String> reviews;

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

	public String getSign() {
		return sign;
	}

	public void setSign(String sign) {
		this.sign = sign;
	}

	public Set<String> getRelativeUsers() {
		return relativeUsers;
	}

	public void setRelativeUsers(Set<String> relativeUsers) {
		this.relativeUsers = relativeUsers;
	}

	public Set<String> getRecommendMovies() {
		return recommendMovies;
	}

	public void setRecommendMovies(Set<String> recommendMovies) {
		this.recommendMovies = recommendMovies;
	}

	public String getDetail() {
		return detail;
	}

	public void setDetail(String detail) {
		this.detail = detail;
	}

	public Set<String> getSeen() {
		return seen;
	}

	public void setSeen(Set<String> seen) {
		this.seen = seen;
	}

	public Set<String> getSeening() {
		return seening;
	}

	public void setSeening(Set<String> seening) {
		this.seening = seening;
	}

	public Set<String> getWantSeens() {
		return wantSeens;
	}

	public void setWantSeens(Set<String> wantSeens) {
		this.wantSeens = wantSeens;
	}

	public Set<String> getReviews() {
		return reviews;
	}

	public void setReviews(Set<String> reviews) {
		this.reviews = reviews;
	}

}
