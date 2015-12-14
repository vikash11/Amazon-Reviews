import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import model.CategoryStat;
import model.FiveReviewerStat;
import model.ReviewerStat;
import model.TopBrandsStats;

public class S3Handler {
	AmazonS3 s3Client;
	String bucketName = "big-data-amazon-reviews";

	public S3Handler() {
		try {
			Properties prop = new Properties();
			InputStream input = new FileInputStream("aws.properties");
			prop.load(input);
			String accesskeyid = prop.getProperty("accesskeyid");
			String accesskeysecret = prop.getProperty("accesskeysecret");
			BasicAWSCredentials awsCreds = new BasicAWSCredentials(accesskeyid, accesskeysecret);
			s3Client = new AmazonS3Client(awsCreds);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void getS3File(String bucketName, String key) {
		try {
			S3Object object = s3Client.getObject(new GetObjectRequest(bucketName, key));
			InputStream objectData = object.getObjectContent();
			objectData.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public ArrayList<TopBrandsStats> getTopBrandsStats(String fileName) {
		ArrayList<TopBrandsStats> topBrandsStats = new ArrayList<TopBrandsStats>();
		try {
			S3Object object = s3Client.getObject(new GetObjectRequest(bucketName, fileName));
			InputStream is = object.getObjectContent();
			BufferedReader reader = new BufferedReader(new InputStreamReader(is));
			String line;
			while ((line = reader.readLine()) != null) {
				TopBrandsStats topBrandsStatsObj = new TopBrandsStats(line);
				topBrandsStats.add(topBrandsStatsObj);
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return topBrandsStats;
	}

	public ArrayList<TopBrandsStats> getTopBrandsFiles() {
		ArrayList<TopBrandsStats> topBrandsStats = new ArrayList<TopBrandsStats>();
		String directory = "data/top-brands-data";
		List<S3ObjectSummary> fileSummaryList = getAllFiles(directory);
		for (S3ObjectSummary objectSummary : fileSummaryList) {
			ArrayList<TopBrandsStats> topBrandsStatsCurrentPart = getTopBrandsStats(objectSummary.getKey());
			topBrandsStats.addAll(topBrandsStatsCurrentPart);
		}
		return topBrandsStats;
	}

	public List<S3ObjectSummary> getAllFiles(String directory) {
		ObjectListing objectListing = s3Client.listObjects(bucketName, directory);
		return objectListing.getObjectSummaries();
	}

	public ArrayList<CategoryStat> getCategoryStats() {
		ArrayList<CategoryStat> categoryStats = new ArrayList<CategoryStat>();
		try {
			String fileName = "data/aggregation-data/part-r-00000";
			S3Object object = s3Client.getObject(new GetObjectRequest(bucketName, fileName));
			InputStream is = object.getObjectContent();
			BufferedReader reader = new BufferedReader(new InputStreamReader(is));
			String line;
			while ((line = reader.readLine()) != null) {
				CategoryStat categoryStat = new CategoryStat(line);
				categoryStats.add(categoryStat);
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return categoryStats;
	}

	private ArrayList<ReviewerStat> getReviewerStatsStats(String fileName, HashMap<String, Set<String>> topReviewers) {
		ArrayList<ReviewerStat> topReviewerStats = new ArrayList<ReviewerStat>();
		try {
			S3Object object = s3Client.getObject(new GetObjectRequest(bucketName, fileName));
			InputStream is = object.getObjectContent();
			BufferedReader reader = new BufferedReader(new InputStreamReader(is));
			String line;
			while ((line = reader.readLine()) != null) {
				ReviewerStat reviewerStat = new ReviewerStat(line);
				if (topReviewers.containsKey(reviewerStat.category)) {
					Set<String> reviewerList = topReviewers.get(reviewerStat.category);
					if (reviewerList.contains(reviewerStat.reviewerId)) {
						topReviewerStats.add(reviewerStat);
					}
				}
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return topReviewerStats;
	}

	public HashMap<String, Set<String>> getTopFiveReviewersHM() {
		HashMap<String, Set<String>> topReviewers = new HashMap<String, Set<String>>();
		ArrayList<FiveReviewerStat> topReviewerStats = new ArrayList<FiveReviewerStat>();
		String directory = "data/top-five-data";
		List<S3ObjectSummary> fileSummaryList = getAllFiles(directory);
		for (S3ObjectSummary objectSummary : fileSummaryList) {
			ArrayList<FiveReviewerStat> topReviewerStatCurrentPart = getTopReviewerStatsStats(objectSummary.getKey(),
					topReviewers);
			topReviewerStats.addAll(topReviewerStatCurrentPart);
		}
		return topReviewers;
	}

	private ArrayList<FiveReviewerStat> getTopReviewerStatsStats(String fileName,
			HashMap<String, Set<String>> topReviewers) {
		ArrayList<FiveReviewerStat> topReviewerStats = new ArrayList<FiveReviewerStat>();
		try {
			S3Object object = s3Client.getObject(new GetObjectRequest(bucketName, fileName));
			InputStream is = object.getObjectContent();
			BufferedReader reader = new BufferedReader(new InputStreamReader(is));
			String line;
			while ((line = reader.readLine()) != null) {
				FiveReviewerStat reviewerStat = new FiveReviewerStat(line);
				Set<String> reviewersList = new HashSet<String>();
				if (topReviewers.containsKey(reviewerStat.category)) {
					reviewersList = topReviewers.get(reviewerStat.category);
				}
				if (reviewersList.size() < 100) {
					reviewersList.add(reviewerStat.reviewerId);
					topReviewers.put(reviewerStat.category, reviewersList);
					topReviewerStats.add(reviewerStat);
				}
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return topReviewerStats;
	}

	public ArrayList<ReviewerStat> getTopFiveReviewersUsingHM(HashMap<String, Set<String>> topReviewers) {
		ArrayList<ReviewerStat> topReviewerStats = new ArrayList<ReviewerStat>();
		String directory = "data/top-reviewers-data";
		List<S3ObjectSummary> fileSummaryList = getAllFiles(directory);
		for (S3ObjectSummary objectSummary : fileSummaryList) {
			ArrayList<ReviewerStat> topReviewerStatCurrentPart = getReviewerStatsStats(objectSummary.getKey(),
					topReviewers);
			topReviewerStats.addAll(topReviewerStatCurrentPart);
		}
		return topReviewerStats;
	}
}
