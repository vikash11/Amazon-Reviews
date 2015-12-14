package org.apache.pig.builtin;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class MetaParser extends EvalFunc<Tuple> {
	public Tuple exec(Tuple tup) throws IOException {
		if (null == tup || tup.size() != 1) {
			return null;
		}
		try {
			ObjectMapper mapper = new ObjectMapper();
			mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
			JsonNode df = mapper.readValue(tup.get(0).toString(), JsonNode.class);

			TupleFactory tf = TupleFactory.getInstance();
			Tuple t = tf.newTuple();

			if (df.get("asin") != null) {
				t.append(df.get("asin").getTextValue());
			} else {
				t.append("");
			}
			if (df.get("categories") != null) {
				JsonNode cat = df.get("categories");
				JsonNode catList = cat.get(0);
				t.append(catList.get(0).getTextValue());
			} else {
				t.append(null);
			}
			if (df.get("brand") != null) {
				t.append(df.get("brand").getTextValue());
			} else {
				t.append(null);
			}
			return t;
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Schema outputSchema(Schema input) {
		Schema tupleSchema = new Schema();
		try {

			tupleSchema.add(new FieldSchema("asin", DataType.CHARARRAY));
			tupleSchema.add(new FieldSchema("category", DataType.CHARARRAY));
			tupleSchema.add(new FieldSchema("brand", DataType.CHARARRAY));
			return new Schema(new FieldSchema(null, tupleSchema, DataType.TUPLE));
		} catch (FrontendException e) {
			e.printStackTrace();
			return null;
		}
	}

	public String getAsin() {
		return "";
	}

	public String getBrand() {
		return "";
	}
}
