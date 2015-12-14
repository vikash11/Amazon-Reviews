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
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class ReviewerParser extends EvalFunc<Tuple> {
	public Tuple exec(Tuple tup) throws IOException {
		if (null == tup || tup.size() != 1) {
			return null;
		}
		try {
			ObjectMapper mapper = new ObjectMapper();
			JsonNode df = mapper.readValue(tup.get(0).toString(), JsonNode.class);

			TupleFactory tf = TupleFactory.getInstance();
			Tuple t = tf.newTuple();

			if (df.get("reviewerID") != null) {
				t.append(df.get("reviewerID").getTextValue());
			} else {
				t.append("");
			}

			if (df.get("asin") != null) {
				t.append(df.get("asin").getTextValue());
			} else {
				t.append("");
			}

			if (df.get("reviewerName") != null) {
				t.append(df.get("reviewerName").getTextValue());
			} else {
				t.append("");
			}
			if (df.get("overall") != null) {
				t.append(Float.parseFloat(df.get("overall").toString()));
			} else {
				t.append(-0.0000);
			}
			if (df.get("helpful") != null) {
				float helpful, overall;
				JsonNode hp = df.get("helpful");
				helpful = Integer.parseInt(hp.get(0).toString());
				overall = Integer.parseInt(hp.get(1).toString());
				t.append(helpful);
				t.append(overall);
				
			} else {
				t.append(-0.0000);
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

			tupleSchema.add(new FieldSchema("reviewerID", DataType.CHARARRAY));
			tupleSchema.add(new FieldSchema("asin", DataType.CHARARRAY));
			tupleSchema.add(new FieldSchema("reviewerName", DataType.CHARARRAY));
			tupleSchema.add(new FieldSchema("overall", DataType.FLOAT));
			tupleSchema.add(new FieldSchema("helpful", DataType.INTEGER));
			tupleSchema.add(new FieldSchema("overallCnt", DataType.INTEGER));
			return new Schema(new FieldSchema(null, tupleSchema, DataType.TUPLE));
		} catch (FrontendException e) {
			e.printStackTrace();
			return null;
		}
	}

	public String getAsin() {
		return "";
	}

	public String getHelpful() {
		return "";
	}
}
