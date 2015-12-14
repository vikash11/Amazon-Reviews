import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import model.TextPair;
import util.FieldsIndex;
import util.FieldsUtil;


public class TopFive extends Configured implements Tool {
public static class TopFiveMapper extends Mapper<LongWritable, Text, TextPair, TextPair> {
	
	private static TextPair reviewerInfo = new TextPair();
	private static TextPair categoryHelpful = new TextPair();
	
		public void map(LongWritable key, Text value, Context context) throws IOException, 
			InterruptedException {
			
			Text reviewerId = new Text();
			Text reviewerName = new Text();
			Text category = new Text();
			Text helpfulScore = new Text();
			
			String str = value.toString();
			if (FieldsUtil.isValid(str)) {
				String reviewerIdStr = FieldsUtil.getFieldValue(str, FieldsIndex.REVIEWER_ID);
				String reviewerNameStr = FieldsUtil.getFieldValue(str, FieldsIndex.REVIEWER_NAME);
				String categoryStr = FieldsUtil.getFieldValue(str, FieldsIndex.CATEGORY);
				String helpfulScoreStr = FieldsUtil.getFieldValue(str, FieldsIndex.REVIEWER_SCORE);
				
				reviewerId.set(reviewerIdStr);
				reviewerName.set(reviewerNameStr);
				category.set(categoryStr);
				helpfulScore.set(helpfulScoreStr);
				
				reviewerInfo.set(reviewerId,reviewerName);
				categoryHelpful.set(category, helpfulScore);
				
				//System.out.println(pair);
				context.write(categoryHelpful, reviewerInfo);
		}
	}
}
	
	public static class TopFiveReducer extends Reducer<TextPair, TextPair, TextPair, TextPair>{
		
		
		public void reduce(TextPair key, Iterable<TextPair> values, Context context) throws IOException,
			InterruptedException {
			
			TextPair reviewerInfo = new TextPair();
			
			for (TextPair val : values) {
				Text reviewerId = new Text(val.getFirst());
				Text reviewerName = new Text(val.getSecond());
				reviewerInfo.set(reviewerId, reviewerName);	
				context.write(key, reviewerInfo);
			}
			//System.out.println(key);
			
		}
	}
	
	
	public static class NaturalKeyGroupingComparator extends WritableComparator {
	    protected NaturalKeyGroupingComparator() {
	        super(TextPair.class, true);
	    }   
	    @SuppressWarnings("rawtypes")
	    @Override
	    public int compare(WritableComparable w1, WritableComparable w2) {
	    	TextPair k1 = (TextPair)w1;
	    	TextPair k2 = (TextPair)w2;
	         
	        return k1.getFirst().compareTo(k2.getFirst());
	    }
	}
	
	public static class CompositeKeyComparator extends WritableComparator {
		public CompositeKeyComparator() {
			super(TextPair.class, true);
		}
	    @SuppressWarnings("rawtypes")
	    @Override
	    public int compare(WritableComparable w1, WritableComparable w2) {
	    	TextPair k1 = (TextPair)w1;
	    	TextPair k2 = (TextPair)w2;
	         
	        int result = k1.getFirst().compareTo(k2.getFirst());
	        if(0 == result) {
	            result = -1* k1.getSecond().compareTo(k2.getSecond());
	        }
	        return result;
	    }
	}
	
	public static class NaturalKeyPartitioner extends Partitioner<TextPair, TextPair> {
		 
	    @Override
	    public int getPartition(TextPair key, TextPair val, int numPartitions) {
	        int hash = key.getFirst().hashCode();
	        int partition = hash % numPartitions;
	        return Math.abs(partition);
	    }
	 
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new TopFive(), args);
	}
	
	@Override
	public int run(String[] args) throws Exception {
			    Configuration conf = new Configuration();
			    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			    if (otherArgs.length < 2) {
			      System.err.println("Usage: topreviews <in> [<in>...] <out>");
			      System.exit(2);
			    }
			    
			    Job job = Job.getInstance(conf, "Top Five Reviews");
			    job.setJarByClass(TopFive.class);
			    job.setPartitionerClass(NaturalKeyPartitioner.class);
				job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
				job.setSortComparatorClass(CompositeKeyComparator.class);
				
			    job.setMapperClass(TopFiveMapper.class);
			    job.setReducerClass(TopFiveReducer.class);
			   
			    job.setMapOutputKeyClass(TextPair.class);
			    job.setMapOutputValueClass(TextPair.class);
			    
			    job.setOutputKeyClass(TextPair.class);
			    job.setOutputValueClass(TextPair.class);
			    
			    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
			    System.exit(job.waitForCompletion(true) ? 0 : 1);
				return 0;
		 }
}

