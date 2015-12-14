package amazon.data.analyze;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
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
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class RangeTop {

	public static class TopratedKey implements WritableComparable<TopratedKey>{
		private String asin;
		private String name;
		private String category;
		private double rating;
		private long noofrating;

		public TopratedKey() {
			asin="";
			name="";
			rating=0.0;
			category="";
			noofrating=0;
		}
		public void setasin(String asin) {
			this.asin = asin;
		}
		public void setname(String name) {
			this.name = name;
		}

		public void setcategory(String category) {
			this.category = category;
		}

		public void setrating(double rating) {
			this.rating = rating;
		}
		public void setnoofrating(long noofrating) {
			this.noofrating = noofrating;
		}
		public String getasin()
		{
			return asin;
		}
		public String getname()
		{
			return name;
		}
		public String getcategory()
		{
			return category;
		}
		public double getrating()
		{
			return rating;
		}
		public long getnoofrating()
		{
			return noofrating;
		}
		@Override
		public void readFields(DataInput in) throws IOException {
			category=in.readUTF();
			asin=in.readUTF();
			name=in.readUTF();
			rating=in.readDouble();
			noofrating=in.readLong();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(category);
			out.writeUTF(asin);
			out.writeUTF(name);
			out.writeDouble(rating);
			out.writeLong(noofrating);
		}

		@Override
		public int compareTo(TopratedKey o) {
			int compare=category.compareTo(o.getcategory());
			if(compare!=0)
				return compare;
			compare=Double.compare(rating, o.getrating());
			if(compare!=0)
				return compare;
			return Long.compare(noofrating, o.getnoofrating());
		}
		@Override
		public String toString(){
			return name+" "+asin+" "+category+" "+rating;
		}
		@Override
		public int hashCode()
		{
			int t=category.hashCode();
			return (t<0)?-t:t;
		}

	}

	public static class ChangeMapper 
	extends Mapper<Object, Text, TopratedKey,Text>{
		int minrange;
		Date start,end;
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
		public void setup(Context context) throws IOException, InterruptedException{
			minrange=context.getConfiguration().getInt("minrange", 1);
			try {
				start = sdf.parse(context.getConfiguration().get("start"));
				end = sdf.parse(context.getConfiguration().get("end"));
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			JSONParser parser = new JSONParser();
			Object infoobj = null;
			try {
				infoobj = parser.parse(value.toString());
			} catch (org.json.simple.parser.ParseException e1) {
				e1.printStackTrace();
			}
			JSONObject infoall = (JSONObject) infoobj;
			String category=(String) infoall.get("category"),name=(String) infoall.get("name"),asin=(String) infoall.get("asin");

			TopratedKey keyp = new TopratedKey();
			keyp.setasin(asin);
			keyp.setname(name);
			keyp.setcategory(category);
			JSONArray allrev = (JSONArray) infoall.get("reviewval");
			Double rating=0.0,cnt=0.0;
			for (int i = 0; i < allrev.size(); i++) {
				Object time = ((JSONObject)allrev.get(i)).get("time");
				Long tl=Long.parseLong(time.toString());
				Date d = new Date(tl);
				if((d.compareTo(start)>=0)&&(d.compareTo(end)<=0))
				{
					rating+=Double.parseDouble((String) ((JSONObject)allrev.get(i)).get("review"));
					cnt++;
				}
			}
			// if there are sufficient no of reviews we add it to our list for calculation
			if(cnt>minrange)
			{
				rating/=cnt;
				keyp.setrating(-rating);
				keyp.setnoofrating((long) -cnt);
				context.write(keyp, new Text(allrev.toJSONString()));
			}
		}
		public void cleanup(Context context) throws IOException, InterruptedException{

		}
	}
	public static class TopProductReducer extends Reducer<TopratedKey,Text,Text,NullWritable> {
		int topk;
		@Override
		protected void setup(Context context) throws IOException,
		InterruptedException {
			topk=context.getConfiguration().getInt("topk", 5);
		}
		public void reduce(TopratedKey key, Iterable<Text> values, 
				Context context
				) throws IOException, InterruptedException {
			// Do some JSON parsing and output the top products
			JSONObject finaljson = new JSONObject();
			finaljson.put("category", key.getcategory());
			JSONArray topitems = new JSONArray();
			int topcnt=0;
			for (Text val : values) {
				JSONObject topjson = new JSONObject();
				topjson.put("asin", key.getasin());
				topjson.put("name", key.getname());
				JSONParser parser = new JSONParser();
				Object infoobj = null;
				try {
					infoobj = parser.parse(val.toString());
				} catch (org.json.simple.parser.ParseException e1) {
					e1.printStackTrace();
				}

				JSONArray temp = (JSONArray) infoobj;
				for (int i = 0; i < temp.size(); i++) {
					JSONObject tt = (JSONObject) temp.get(i);
					Object time = tt.get("time");
					Long tl=Long.parseLong(time.toString());
					Date d = new Date(tl);
				}
				topjson.put("reviewal", temp);
				topitems.add(topjson);
				topcnt++;
				if(topcnt==topk)
					break;
			}
			finaljson.put("top", topitems);
			context.write(new Text(finaljson.toJSONString()), null);
		}
	}
	public static class CategoryPartitioner extends Partitioner<TopratedKey, Text> {
		// which makes sure every key with same category goes to same Reducer, done using Hashcode
		@Override
		public int getPartition(TopratedKey key, Text value, int numPartitions) {
			return key.hashCode()%numPartitions;
		}
	}
	public static class GroupComparator extends WritableComparator {
		// groups every key with same category and sends them to one reduce call

		protected GroupComparator() {
			super(TopratedKey.class, true);
		}
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			return ((TopratedKey) w1).getcategory().compareTo(((TopratedKey) w2).getcategory());
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: RangeTop <startdate> <enddate> <minimum no of reviews> <top output limit> <in> <out>");
			System.exit(2);
		}
		conf.set("start", otherArgs[0]); // get the start date from input parameter and set it in job configuration
		conf.set("end", otherArgs[1]);   // get the end date from input parameter and set it in job configuration
		conf.setInt("minrange", Integer.parseInt(otherArgs[2])); // get the minimum no of reviews from input parameter and set it in job configuration
		conf.setInt("topk", Integer.parseInt(otherArgs[3])); // get the top output limit from input parameter and set it in job configuration

		Job job = Job.getInstance(conf, "delay calculate");
		job.setJarByClass(RangeTop.class);
		job.setMapperClass(ChangeMapper.class);
		job.setMapOutputKeyClass(TopratedKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(10);
		job.setPartitionerClass(CategoryPartitioner.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setReducerClass(TopProductReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[4]));
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length - 1]));
		System.out.println(job.waitForCompletion(true) ? 0 : 1);
	}
}

