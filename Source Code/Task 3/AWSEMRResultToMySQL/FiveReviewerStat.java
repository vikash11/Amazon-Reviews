package model;

public class FiveReviewerStat {
	public String reviewerId;
	public String reviewerName;
	public String category;
	public double reviewerScore;

	public FiveReviewerStat(String category) {
		System.out.println(category);
		String[] splitted = category.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
		this.category = splitted[0].trim();
		this.reviewerId = splitted[2].trim();
		try {
			reviewerScore = Double.parseDouble(splitted[1]);
		} catch (java.lang.NumberFormatException nfe) {
			reviewerScore = 0;
		}
		if (splitted.length >= 4) {
			this.reviewerName = splitted[3].trim();
		} else {
			this.reviewerName = "";
		}
	}
}
