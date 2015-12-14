package amazon.data.setup;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class Join {

	public static class ReviewKey implements WritableComparable<ReviewKey>{
		private String asin;
		private long time;

		public ReviewKey() {
			asin="";
			time=0;
		}
		public void setasin(String asin) {
			this.asin = asin;
		}

		public void settime(long time) {
			this.time = time;
		}
		public String getasin()
		{
			return asin;
		}
		public long gettime()
		{
			return time;
		}
		@Override
		public void readFields(DataInput in) throws IOException {
			asin=in.readUTF();
			time=in.readLong();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(asin);
			out.writeLong(time);
		}

		@Override
		public int compareTo(ReviewKey o) {
			int compare=asin.compareTo(o.getasin());
			if(compare!=0)
				return compare;
			return Long.compare(time, o.gettime());
		}
		@Override
		public String toString(){
			return asin+" "+time;
		}
		@Override
		public int hashCode()
		{
			int t=asin.hashCode();
			return (t<0)?-t:t;
		}

	}
	// Mapper for meta data
	public static class MetaMapper 
	extends Mapper<Object, Text, ReviewKey,Text>{
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			JSONParser parser = new JSONParser();
			Object metaobj = null;
			try {
				metaobj = parser.parse(value.toString());
			} catch (org.json.simple.parser.ParseException e1) {
				e1.printStackTrace();
			}
			JSONObject metall = (JSONObject) metaobj;
			if(metall!=null)
			{
				ReviewKey metakey = new ReviewKey();
				String val="";
				metakey.setasin(metall.get("asin")+"");
				metakey.settime(0);
				String name="",categorystring="";
				if(metall.containsKey("categories"))
				{
				JSONArray categories = (JSONArray) metall.get("categories");
				JSONArray category = (JSONArray) categories.get(0);
				if(category.get(0)!=null)
					categorystring=category.get(0).toString();
				else
					categorystring="Category Not available";
				}
				else
					categorystring="Category Not available";
				if(metall.get("title")==null)
					name="Name not available";
				else
				{
					name=metall.get("title").toString();
					name.replaceAll("\t", "");
				}
				val+=name+"@@"+categorystring;
				context.write(metakey,new Text(val));
			}
		}
	}
	// Mapper for review data
	public static class ReviewMapper 
	extends Mapper<Object, Text, ReviewKey,Text>{
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			JSONParser parser = new JSONParser();
			Object reviewobj = null;
			try {
				reviewobj = parser.parse(value.toString());
			} catch (org.json.simple.parser.ParseException e1) {
				e1.printStackTrace();
			}
			JSONObject revall = (JSONObject) reviewobj;
			ReviewKey revkey = new ReviewKey();

			revkey.setasin((String) revall.get("asin"));
			String keytime="";
			keytime+=revall.get("unixReviewTime");
			revkey.settime(Long.parseLong(keytime));
			String reqval="";
			reqval+=revall.get("overall");
			context.write(revkey, new Text(reqval+""));
		}
	}
	public static class MetaReviewJoiner 
	extends Reducer<ReviewKey,Text,Text,NullWritable> {
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
		@Override
		protected void setup(Context context) throws IOException,
		InterruptedException {

		}
		public void reduce(ReviewKey key, Iterable<Text> values, 
				Context context
				) throws IOException, InterruptedException {

			boolean nameset=false;
			String name="";
			String category="";
			int cnt=0;
			JSONObject compjson = new JSONObject();
			JSONArray comparr=new JSONArray();
			for (Text val : values) {
				if(!nameset)
				{
					if(key.gettime()!=0)
						break;
					String nc[]=val.toString().split("@@");
					name=nc[0];
					category=nc[1];
					compjson.put("asin", key.getasin());
					compjson.put("name", name);
					compjson.put("category", category);
					if(key.gettime()==0)
						nameset=true;
					continue;
				}
				if(key.gettime()!=0){
				JSONObject temp = new JSONObject();
				temp.put("review", val.toString());
				temp.put("time", (key.gettime()+60*60*24)*1000);
				comparr.add(temp);
				cnt++;
				}
			}
			if((cnt!=0)&&nameset)
			{
				compjson.put("reviewval", comparr);
				context.write(new Text(compjson.toJSONString()), null);
			}
		}
	}
	public static class AsinPartitioner extends Partitioner<ReviewKey, Text> {
		@Override
		public int getPartition(ReviewKey key, Text value, int numPartitions) {
			return key.hashCode()%numPartitions;
		}
	}
	public static class GroupComparator extends WritableComparator {
		protected GroupComparator() {
			super(ReviewKey.class, true);
		}
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			return ((ReviewKey) w1).getasin().compareTo(((ReviewKey) w2).getasin());
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 3) {
			System.err.println("Usage: DelayCalculate <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "delay calculate");
		job.setJarByClass(Join.class);
		job.setMapOutputKeyClass(ReviewKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setPartitionerClass(AsinPartitioner.class);
		job.setNumReduceTasks(10);
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setReducerClass(MetaReviewJoiner.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, MetaMapper.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, ReviewMapper.class);
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length - 1]));
		System.out.println(job.waitForCompletion(true) ? 0 : 1);
	}
}

