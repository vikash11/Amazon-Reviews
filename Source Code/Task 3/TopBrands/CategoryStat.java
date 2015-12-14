package model;

public class CategoryStat {
	public String category = "";
	public double overall = 0;
	public double helpfulYes = 0;
	public double helpfulCount = 0;
	public double categoryCount = 0;

	public CategoryStat() {

	}

	public CategoryStat(String category) {
		String[] splitted = category.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
		this.category = splitted[0];
		try {
			overall = Double.parseDouble(splitted[1]);
		} catch (java.lang.NumberFormatException nfe) {
			overall = 0;
		}
		try {
			helpfulYes = Double.parseDouble(splitted[2]);
		} catch (java.lang.NumberFormatException nfe) {
			helpfulYes = 0;
		}
		try {
			helpfulCount = Double.parseDouble(splitted[3]);
		} catch (java.lang.NumberFormatException nfe) {
			helpfulCount = 0;
		}
		try {
			categoryCount = Double.parseDouble(splitted[4]);
		} catch (java.lang.NumberFormatException nfe) {
			categoryCount = 0;
		}
	}
}
