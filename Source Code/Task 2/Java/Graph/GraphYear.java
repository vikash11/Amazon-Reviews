import java.io.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.data.time.Day;
import org.jfree.data.time.RegularTimePeriod;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.time.TimeSeriesDataItem;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.XYSeriesCollection;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.jfree.chart.ChartUtilities; 

public class GraphYear
{
	public static void main( String[ ] args )throws Exception
	{
		try {
			FileReader tmp = new FileReader("%path to YearWise reducers output%");
			BufferedReader tmpbr = new BufferedReader(tmp);
			String sCurrentLine;
			while ((sCurrentLine = tmpbr.readLine()) != null) {
				JSONParser parser = new JSONParser();
				Object infoobj = null;
				try {
					infoobj = parser.parse(sCurrentLine);
				} catch (org.json.simple.parser.ParseException e1) {
					e1.printStackTrace();
				}
				JSONObject infoall = (JSONObject) infoobj;
				String asin=(String) infoall.get("asin");
				String info="Category:"+infoall.get("category")+" ASIN:"+asin+" Name:"+infoall.get("name")+" Avgrating:"+infoall.get("avgrating")+" Total Count:"+infoall.get("totcount");
				JSONArray yearall =(JSONArray) infoall.get("yeardet");
				TimeSeries rating = new TimeSeries("Rating");
				TimeSeries count = new TimeSeries("Count");
				for (int i = 0; i < yearall.size(); i++) {
					JSONObject oney= (JSONObject) yearall.get(i);
					Object tobj = oney.get("yearavgrating");
					Double yr=Double.parseDouble(tobj.toString());
					tobj = oney.get("year");
					Double year=Double.parseDouble(tobj.toString());
					tobj = oney.get("count");
					Double yc=Double.parseDouble(tobj.toString());
					rating.add(new Day(1,1,year.intValue()), yr);
					count.add(new Day(1,1,year.intValue()), yc);
				}
				TimeSeriesCollection datasetcount = new TimeSeriesCollection();
				TimeSeriesCollection datasetrating = new TimeSeriesCollection();
				datasetcount.addSeries(count);
				datasetrating.addSeries(rating);
				String xAxisLabel = "Year";
				JFreeChart chart = ChartFactory.createTimeSeriesChart(info,xAxisLabel, "count", datasetcount);
				int width = 640; /* Width of the image */
				int height = 480; /* Height of the image */ 
				File XYChart = new File( "graphs/"+asin+"count.jpeg" ); 
				ChartUtilities.saveChartAsJPEG( XYChart, chart, width, height);
				chart = ChartFactory.createTimeSeriesChart(info,xAxisLabel, "rating", datasetrating);
				XYChart = new File( "graphs/"+asin+"rating.jpeg" ); 
				ChartUtilities.saveChartAsJPEG( XYChart, chart, width, height);
			}	
			tmpbr.close();
			tmp.close();
		}catch(Exception e){
			e.printStackTrace();
		}



	}
}