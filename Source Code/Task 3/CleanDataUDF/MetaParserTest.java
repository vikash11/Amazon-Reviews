package org.apache.pig.builtin;

import java.io.IOException;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class MetaParserTest {
	public static void main(String args[]) {

		try {
			ObjectMapper mapper = new ObjectMapper();
			mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
			JsonNode df = mapper.readValue(
					"{'asin': '0132793040', 'imUrl': 'http://ecx.images-amazon.com/images/I/31JIPhp%2BGIL.jpg', 'description': 'The Kelby Training DVD Mastering Blend Modes in Adobe Photoshop CS5 with Corey Barker is a useful tool for becoming familiar with the use of blend modes in Adobe Photoshop. For those who are serious about mastering all that Photoshop has to offer, mastering blend modes is just as important as mastering layers.In this DVD tutorial, seasoned expert Corey Barker explores the function of blend modes in a variety of scenarios such as image restoration, sharpening, adjustments, special effects and much more. Since every project scenario is different, Corey encourages you to experiment with these blend modes by giving you the skills and confidence you need.', 'categories': [['Electronics', 'Computers & Accessories', 'Cables & Accessories', 'Monitor Accessories']], 'title': 'Kelby Training DVD: Mastering Blend Modes in Adobe Photoshop CS5 By Corey Barker'}",
					JsonNode.class);
			System.out.println(df.get("asin"));
			
			JsonNode cat = df.get("categories");
			JsonNode catList = cat.get(0);
			
			System.out.println(catList.get(0));
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
