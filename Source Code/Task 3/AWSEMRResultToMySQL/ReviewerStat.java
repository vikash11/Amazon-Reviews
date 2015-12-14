package model;

public class ReviewerStat {
	public String reviewerId;
	public String reviewerName;
	public String category;
	public double avgRating;
	public double numberOfReviews;
	public double reviewerScore;

	public ReviewerStat(String category) {
		System.out.println(category);
		String[] splitted = category.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
		this.reviewerId = splitted[0].trim();
		this.reviewerName = splitted[1].trim();
		this.category = splitted[2].trim();
		try {
			avgRating = Double.parseDouble(splitted[3]);
		} catch (java.lang.NumberFormatException nfe) {
			avgRating = 0;
		}
		try {
			numberOfReviews = Double.parseDouble(splitted[4]);
		} catch (java.lang.NumberFormatException nfe) {
			numberOfReviews = 0;
		}
		try {
			reviewerScore = Double.parseDouble(splitted[5]);
		} catch (java.lang.NumberFormatException nfe) {
			reviewerScore = 0;
		}
	}
}
