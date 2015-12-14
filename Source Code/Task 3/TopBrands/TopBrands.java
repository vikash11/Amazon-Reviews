import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import model.BrandCategoryPair;
import model.BrandStatistics;
import model.CategoryData;
import model.CategoryStat;
import util.FieldsIndex;
import util.FieldsUtil;

public class TopBrands {

	public static class TokenizerMapper extends Mapper<Object, Text, BrandCategoryPair, CategoryData> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String str = value.toString();

			// System.out.println(str);
			if (FieldsUtil.isValid(str)) {
				String brand = FieldsUtil.getFieldValue(str, FieldsIndex.BRAND);
				String category = FieldsUtil.getFieldValue(str, FieldsIndex.CATEGORY);
				String rating = FieldsUtil.getFieldValue(str, FieldsIndex.OVERALL_RATING);
				// System.out.println(brand + " " + category + " " + rating);
				try {
					double ratingD = Double.parseDouble(rating);
					DoubleWritable ratingDW = new DoubleWritable(ratingD);
					DoubleWritable countOneDW = new DoubleWritable(1);
					List<DoubleWritable> overallRatingAndCount = new ArrayList<DoubleWritable>();
					overallRatingAndCount.add(ratingDW);
					overallRatingAndCount.add(countOneDW);

					BrandCategoryPair brandCategoryPair = new BrandCategoryPair(brand, category);
					CategoryData categoryData = new CategoryData(countOneDW, ratingDW);
					context.write(brandCategoryPair, categoryData);
				} catch (NumberFormatException nfe) {

				}
			}
		}
	}

	public static class IntSumReducer
			extends Reducer<BrandCategoryPair, CategoryData, BrandCategoryPair, BrandStatistics> {

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

		public void reduce(BrandCategoryPair brandCategoryPair, Iterable<CategoryData> overallRatingAndCountIter,
				Context context) throws IOException, InterruptedException {
			double rating = 0;
			double count = 0;
			for (CategoryData val : overallRatingAndCountIter) {
				rating += val.getTotalRating().get();
				count += val.getTotalCount().get();
			}
			double avgRating = rating / count;
			double minimumRatingCount = 1000;
			// R = average rating for the brand
			// v = number of reviews for the brand
			// m = minimum reviews required (1000)
			// C = average rating for the category
			// (WR) = (v ÷ (v+m)) × R + (m ÷ (v+m)) × C
			CategoryStat categoryStat = new CategoryStat();
			if (categoryStats.containsKey(brandCategoryPair.getCategory().toString())) {
				categoryStat = categoryStats.get(brandCategoryPair.getCategory().toString());
			}
			double brandRating = (((count / (count + minimumRatingCount)) * avgRating)
					+ ((minimumRatingCount / (count + minimumRatingCount)) * categoryStat.overall));
			if (count >= minimumRatingCount) {
				DoubleWritable brandAvgRating = new DoubleWritable(avgRating);
				DoubleWritable brandNumberOfReviews = new DoubleWritable(count);
				DoubleWritable brandScore = new DoubleWritable(brandRating);
				BrandStatistics brandStatistics = new BrandStatistics(brandAvgRating, brandNumberOfReviews, brandScore);
				context.write(brandCategoryPair, brandStatistics);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Top Brands Analyze");
		job.setJarByClass(TopBrands.class);
		job.setMapperClass(TokenizerMapper.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);

		job.setMapOutputKeyClass(BrandCategoryPair.class);
		job.setMapOutputValueClass(CategoryData.class);

		job.setOutputKeyClass(BrandCategoryPair.class);
		job.setOutputValueClass(BrandStatistics.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}