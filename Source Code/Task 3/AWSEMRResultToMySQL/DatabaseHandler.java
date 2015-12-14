import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;

import model.CategoryStat;
import model.FiveReviewerStat;
import model.ReviewerStat;
import model.TopBrandsStats;

public class DatabaseHandler {
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";

	Connection conn = null;
	Statement stmt = null;

	public DatabaseHandler() {
		try {
			Properties prop = new Properties();
			InputStream input = new FileInputStream("mysql.properties");
			prop.load(input);
			String endpoint = prop.getProperty("endpoint");
			String username = prop.getProperty("username");
			String password = prop.getProperty("password");
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(endpoint, username, password);
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("CONNECTION SUCCESSFULL");
	}

	public void insertCategoryStats(ArrayList<CategoryStat> categoryStats) {
		try {
			stmt = conn.createStatement();
			String query = "insert into category_stats (category, overall, helpful_yes, helpful_count, category_count) values (?,?,?,?,?)";
			PreparedStatement preparedStmt = conn.prepareStatement(query);
			for (CategoryStat categoryStat : categoryStats) {
				preparedStmt.setString(1, categoryStat.category);
				preparedStmt.setDouble(2, categoryStat.overall);
				preparedStmt.setDouble(3, categoryStat.helpfulYes);
				preparedStmt.setDouble(4, categoryStat.helpfulCount);
				preparedStmt.setInt(5, categoryStat.categoryCount);
				preparedStmt.addBatch();
			}
			preparedStmt.executeBatch();
			preparedStmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void insertTopBrandsStats(ArrayList<TopBrandsStats> topBrandsStats) {
		try {
			stmt = conn.createStatement();
			String query = "insert into top_brands_stats (brand, category, avg_rating, number_of_reviews, brand_score) values (?,?,?,?,?)";
			PreparedStatement preparedStmt = conn.prepareStatement(query);
			for (TopBrandsStats topBrandsStat : topBrandsStats) {
				preparedStmt.setString(1, topBrandsStat.brand);
				preparedStmt.setString(2, topBrandsStat.category);
				preparedStmt.setDouble(3, topBrandsStat.brandAvgRating);
				preparedStmt.setDouble(4, topBrandsStat.brandNumberOfReviews);
				preparedStmt.setDouble(5, topBrandsStat.brandScore);
				preparedStmt.addBatch();
			}
			preparedStmt.executeBatch();
			preparedStmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void insertTopReviewerStat(ArrayList<ReviewerStat> topReviewersStats) {
		try {
			stmt = conn.createStatement();
			String query = "insert into reviewer_stats (reviewer_aid, reviewer_name, category, avg_rating, number_of_reviews, reviewer_score) values (?,?,?,?,?,?)";
			PreparedStatement preparedStmt = conn.prepareStatement(query);
			int count = 0;
			for (ReviewerStat reviewerStat : topReviewersStats) {
				preparedStmt.setString(1, reviewerStat.reviewerId);
				preparedStmt.setString(2, reviewerStat.reviewerName);
				preparedStmt.setString(3, reviewerStat.category);
				preparedStmt.setDouble(4, reviewerStat.avgRating);
				preparedStmt.setDouble(5, reviewerStat.numberOfReviews);
				preparedStmt.setDouble(6, reviewerStat.reviewerScore);
				preparedStmt.addBatch();
				count++;
				System.out.println("Inserted " + count);
			}
			preparedStmt.executeBatch();
			preparedStmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
