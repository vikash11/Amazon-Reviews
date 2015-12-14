package model;

public class TopBrandsStats {
	public String category;
	public String brand;
	public double brandAvgRating;
	public double brandNumberOfReviews;
	public double brandScore;

	public TopBrandsStats(String topBrandStatsStr) {
		System.out.println(topBrandStatsStr);
		String[] splitted = topBrandStatsStr.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
		this.brand = splitted[0];
		this.category = splitted[1].trim();
		try {
			brandAvgRating = Double.parseDouble(splitted[2]);
		} catch (java.lang.NumberFormatException nfe) {
			brandAvgRating = 0;
		}
		try {
			brandNumberOfReviews = Double.parseDouble(splitted[3]);
		} catch (java.lang.NumberFormatException nfe) {
			brandNumberOfReviews = 0;
		}
		try {
			brandScore = Double.parseDouble(splitted[4]);
		} catch (java.lang.NumberFormatException nfe) {
			brandScore = 0;
		}
	}
}
