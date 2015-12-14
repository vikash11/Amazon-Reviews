package amazon.data.analyze;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

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

import com.google.gson.JsonArray;

public class YearWise {

	public static class ChangeMapper 
	extends Mapper<Object, Text, Text,NullWritable>{
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
		public void setup(Context context) throws IOException, InterruptedException{

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
            JSONObject result = new JSONObject();
            result.put("asin", asin);
            result.put("name", name);
            result.put("category", category);
			JSONArray allrev = (JSONArray) infoall.get("reviewval");
			HashMap<Integer, Double> count = new HashMap<Integer, Double>();
			HashMap<Integer, Double> rating = new HashMap<Integer, Double>();
			HashMap<Integer, JSONArray> jsonyear = new HashMap<Integer, JSONArray>();
			Double avgrating=0.0,totcnt=0.0;
			for (int i = 0; i < allrev.size(); i++) {
				Object time = ((JSONObject)allrev.get(i)).get("time");
				Long tl=Long.parseLong(time.toString());
				Date d = new Date(tl);
				int year=d.getYear()+1900;
				double ocount=0.0,orating=0.0;
				JSONArray t = new JSONArray();
				if(count.containsKey(year))
				{
				 ocount=count.get(year);
				 orating=rating.get(year);
				 t=jsonyear.get(year);
				}
				ocount++;
				totcnt++;
				try{
					Double.parseDouble((String) ((JSONObject)allrev.get(i)).get("review"));
				}
				catch(Exception e){
					System.out.println(allrev.get(i));	
				}
				orating+=Double.parseDouble((String) ((JSONObject)allrev.get(i)).get("review"));
				avgrating+=Double.parseDouble((String) ((JSONObject)allrev.get(i)).get("review"));
				t.add(((JSONObject)allrev.get(i)));
				count.put(year, ocount);
				rating.put(year, orating);
				jsonyear.put(year, t);
			}
			avgrating/=totcnt;
			result.put("totcount", totcnt);
			result.put("avgrating", avgrating);
			JSONArray allyear = new JSONArray();
			for (Integer keyt:count.keySet()) {
				JSONObject oneyear = new JSONObject();
				oneyear.put("year", keyt);
				Double ocount=count.get(keyt);
				Double orating=rating.get(keyt);
				orating/=ocount;
				JSONArray allyearrev=jsonyear.get(keyt);
				oneyear.put("count", ocount);
				oneyear.put("yearavgrating", orating);
				oneyear.put("revallyear", allyearrev);
				allyear.add(oneyear);
			}
			result.put("yeardet", allyear);
			context.write(new Text(result.toJSONString()), null);
		}
		public void cleanup(Context context) throws IOException, InterruptedException{

		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: DelayCalculate <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "delay calculate");
		job.setJarByClass(YearWise.class);
		job.setMapperClass(ChangeMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length - 1]));
		System.out.println(job.waitForCompletion(true) ? 0 : 1);
	}
}

