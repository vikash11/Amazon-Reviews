import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.data.time.Day;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.XYSeriesCollection;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.jfree.chart.ChartUtilities; 

public class GraphChange
{
	public static void main( String[ ] args )throws Exception
	{
		try {
			FileReader tmp = new FileReader("%path to ChangeTop reducers output%");
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
				JSONObject dummy = (JSONObject) infoobj;
				String category=(String) dummy.get("category");
				JSONArray allprod =(JSONArray) dummy.get("top");
				for (int j = 0; j < allprod.size(); j++) {
					JSONObject infoall = (JSONObject) allprod.get(j);
					String asin=(String) infoall.get("asin");
					
					File file = new File("graphs/change/"+category);
					if (!file.exists()) {
						if (file.mkdir()) {
							System.out.println("Directory is created!");
						} else {
							System.out.println("Failed to create directory!");
						}
					}
					String info=" ASIN:"+asin+" Name:"+infoall.get("name");
					TimeSeries rating = new TimeSeries("Rating");
					JSONArray yearall =(JSONArray) infoall.get("reviewal");
					for (int i = 0; i < yearall.size(); i++) {
						JSONObject oney= (JSONObject) yearall.get(i);
						Object revo= oney.get("review");
						Double rev=Double.parseDouble(revo.toString());
						revo = oney.get("time");
						Long tl= Long.parseLong(revo.toString());
						Date temp = new Date(tl);
						rating.addOrUpdate(new Day(temp), rev);
					}
					TimeSeriesCollection datasetrating = new TimeSeriesCollection();
					datasetrating.addSeries(rating);
					String xAxisLabel = "Year";
					int width = 640; /* Width of the image */
					int height = 480; /* Height of the image */ 
					JFreeChart chart = ChartFactory.createTimeSeriesChart(info,xAxisLabel, "rating", datasetrating);
					File XYChart = new File( "graphs/change/"+category+"/"+asin+"rating.jpeg" ); 
					ChartUtilities.saveChartAsJPEG( XYChart, chart, width, height);
				}	
			}
			tmpbr.close();
			tmp.close();
		}catch(Exception e){
			e.printStackTrace();
		}



	}
}