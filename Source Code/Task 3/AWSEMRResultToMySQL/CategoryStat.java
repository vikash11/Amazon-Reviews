package model;

public class CategoryStat {
	public String category;
	public double overall;
	public double helpfulYes;
	public double helpfulCount;
	public int categoryCount;

	public CategoryStat(String category) {
		System.out.println(category);
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
			categoryCount = Integer.parseInt(splitted[4]);
		} catch (java.lang.NumberFormatException nfe) {
			categoryCount = 0;
		}
	}
}
