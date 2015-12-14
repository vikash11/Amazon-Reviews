package org.apache.pig.builtin;

import java.io.IOException;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;


public class ReviewerParserTest {
	public static void main(String args[]) {

		try {
			ObjectMapper mapper = new ObjectMapper();
			float helpful, overall;
			JsonNode df = mapper.readValue(
					"{\"reviewerID\": \"AKM1MP6P0OYPR\", \"asin\": \"0132793040\", \"reviewerName\": \"Vicki Gibson momo4\", \"helpful\": [1, 2], \"reviewText\": \"Corey Barker does a great job of explaining Blend Modes in this DVD. All of the Kelby training videos are great but pricey to buy individually. If you really want bang for your buck just subscribe to Kelby Training online.\", \"overall\": 5.0, \"summary\": \"Very thorough\", \"unixReviewTime\": 1365811200, \"reviewTime\": \"04 13, 2013\"}",
					JsonNode.class);
			System.out.println(df.get("asin"));
			JsonNode hp = df.get("helpful");
			helpful = Integer.parseInt(hp.get(0).toString());
			overall = Integer.parseInt(hp.get(1).toString());
			System.out.println(helpful + "\n" + overall);
			
		} catch (

		JsonParseException e)

		{
			e.printStackTrace();
		} catch (

		JsonMappingException e)

		{
			e.printStackTrace();
		} catch (

		IOException e)

		{
			e.printStackTrace();
		}
	}
}
