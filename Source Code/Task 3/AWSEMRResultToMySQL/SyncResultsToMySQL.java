import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import model.CategoryStat;
import model.FiveReviewerStat;
import model.ReviewerStat;
import model.TopBrandsStats;

public class SyncResultsToMySQL {

	public static void main(String[] args) {
		S3Handler s3Handler = new S3Handler();
		DatabaseHandler databaseHandler = new DatabaseHandler();
		// syncCategories(s3Handler, databaseHandler);
		// syncTopBrands(s3Handler, databaseHandler);
		syncTopFiveReviewersUsingHM(s3Handler, databaseHandler);
	}

	private static void syncTopBrands(S3Handler s3Handler, DatabaseHandler databaseHandler) {
		ArrayList<TopBrandsStats> topBrandsStats = s3Handler.getTopBrandsFiles();
		System.out.println("Received top brands stats.");
		databaseHandler.insertTopBrandsStats(topBrandsStats);
		System.out.println("Inserted top brands stats.");
	}

	private static void syncCategories(S3Handler s3Handler, DatabaseHandler databaseHandler) {
		ArrayList<CategoryStat> categoryStats = s3Handler.getCategoryStats();
		System.out.println("Received category stats.");
		databaseHandler.insertCategoryStats(categoryStats);
		System.out.println("Inserted category stats.");
	}

	private static void syncTopFiveReviewersUsingHM(S3Handler s3Handler, DatabaseHandler databaseHandler) {
		HashMap<String, Set<String>> topReviewers = s3Handler.getTopFiveReviewersHM();
		ArrayList<ReviewerStat> topReviewersStats = s3Handler.getTopFiveReviewersUsingHM(topReviewers);
		System.out.println("Received top reviewers stats " + topReviewersStats.size());
		databaseHandler.insertTopReviewerStat(topReviewersStats);
		System.out.println("Inserted top reviewers stats.");
	}

}
