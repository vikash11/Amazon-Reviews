import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import model.CategoryStat;
import model.ReviewerStatistics;
import model.TextPair;
import model.ReviewerInfo;
import util.FieldsIndex;
import util.FieldsUtil;

public class TopReviewers {

	public static class TokenizerMapper extends Mapper<LongWritable, Text, ReviewerInfo, TextPair> {

		private static ReviewerInfo reviewerInfo = new ReviewerInfo();
		private static TextPair helpfulData = new TextPair();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			Text reviewerId = new Text();
			Text reviewerName = new Text();
			Text category = new Text();
			Text helpful = new Text();
			Text overall = new Text();

			String str = value.toString();
			if (FieldsUtil.isValid(str)) {
				String reviewerIdStr = FieldsUtil.getFieldValue(str, FieldsIndex.REVIEWER_ID);
				String reviewerNameStr = FieldsUtil.getFieldValue(str, FieldsIndex.REVIEWER_NAME);
				String categoryStr = FieldsUtil.getFieldValue(str, FieldsIndex.CATEGORY);
				String helpfulStr = FieldsUtil.getFieldValue(str, FieldsIndex.HELPFULLNESS_YES);
				String overallStr = FieldsUtil.getFieldValue(str, FieldsIndex.HELPFULLNESS_COUNT);

				reviewerId.set(reviewerIdStr);
				reviewerName.set(reviewerNameStr);
				category.set(categoryStr);
				helpful.set(helpfulStr);
				overall.set(overallStr);
				reviewerInfo.set(reviewerId,reviewerName, category);
				helpfulData.set(helpful, overall);
				context.write(reviewerInfo, helpfulData);
			}
		}
	}

	public static class FloatSumReducer extends Reducer<ReviewerInfo, TextPair, ReviewerInfo, ReviewerStatistics> {

		private HashMap<String, CategoryStat> categoryStats = new HashMap<String, CategoryStat>();

		public void setup(Context context) throws IOException {
			List<String> categoryData = getCategoryData();
			for (String category : categoryData) {
				CategoryStat categoryStat = new CategoryStat(category);
				categoryStats.put(categoryStat.category, categoryStat);
			}
		}
		
		public List<String> getCategoryData() {
			List<String> categoryData = new ArrayList<String>();
			try {
				URL oracle = new URL(
						"https://s3-us-west-2.amazonaws.com/big-data-amazon-reviews/data/aggregation-data/part-r-00000");
				BufferedReader in = new BufferedReader(new InputStreamReader(oracle.openStream()));
				String inputLine;
				while ((inputLine = in.readLine()) != null)
					categoryData.add(inputLine);
				in.close();
			} catch (MalformedURLException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return categoryData;
		}

		public void reduce(ReviewerInfo key, Iterable<TextPair> values, Context context)
				throws IOException, InterruptedException {

			float helpful = 0;
			float overall = 0;
			float unhelpful = 0;
			float totalHelpful = 0;
			float totalOverall = 0;
			int cnt = 0;
			for (TextPair val : values) {
				
				helpful = Float.parseFloat(val.getFirst().toString());
				overall = Float.parseFloat(val.getSecond().toString());
				unhelpful += (helpful - overall);
				totalHelpful += helpful;
				totalOverall += overall;
				cnt++;
			}
			double avgRating = ((2*totalHelpful) - totalOverall) / cnt;
			double helpfulRating = totalHelpful / cnt;
			double unhelpfulRating = unhelpful / cnt;
			double minReviewCount = 20;
			// R = average helpfulness of a reviewer
			// U = average unhelpfulness of a reviewer
			// U = sum of the positive helpfulness votes minus the sum of
			//     the unhelpful votes
			// v = number of helpful rating for a reviewer
			// m = minimum reviews required (5)
			// C = average rating for the category
			// (WR) = (v ÷ (v+m)) × R + - (v ÷ (v+m)) × U + (m ÷ (v+m)) × C
			CategoryStat categoryStat = new CategoryStat();
			if (categoryStats.containsKey(key.getCategory().toString())) {
				categoryStat = categoryStats.get(key.getCategory().toString());
			}
			double reviewerRating = (((cnt / (cnt + minReviewCount)) * helpfulRating) - ((cnt / (cnt + minReviewCount)) * unhelpfulRating)
					+ ((minReviewCount / (cnt + minReviewCount)) * categoryStat.overall));
			
			if (cnt >= minReviewCount) {
				DoubleWritable reviewerAvgRating = new DoubleWritable(avgRating);
				DoubleWritable reviewerNumberOfReviews = new DoubleWritable(cnt);
				DoubleWritable reviewerScore = new DoubleWritable(reviewerRating);
				ReviewerStatistics reviewerStatistics = new ReviewerStatistics(reviewerAvgRating, reviewerNumberOfReviews, reviewerScore);
				context.write(key, reviewerStatistics);
				}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: topreviews <in> [<in>...] <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "Top Reviews");
		job.setJarByClass(TopReviewers.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(FloatSumReducer.class);

		job.setMapOutputKeyClass(ReviewerInfo.class);
		job.setMapOutputValueClass(TextPair.class);

		job.setOutputKeyClass(TextPair.class);
		job.setOutputValueClass(ReviewerStatistics.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}