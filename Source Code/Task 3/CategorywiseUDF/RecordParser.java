package org.apache.pig.builtin;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import util.FieldsIndex;
import util.FieldsUtil;

public class RecordParser extends EvalFunc<Tuple> {

	public Tuple exec(Tuple tup) throws IOException {
		if (null == tup || tup.size() != 1) {
			return null;
		}
		try {
			TupleFactory tf = TupleFactory.getInstance();
			Tuple t = tf.newTuple();
			String str = tup.get(0).toString();
			if (FieldsUtil.isValid(str)) {
				// String asin = FieldsUtil.getFieldValue(str,
				// FieldsIndex.ASIN);
				// String reviewerID = FieldsUtil.getFieldValue(str,
				// FieldsIndex.REVIEWER_ID);
				// String reviewerName = FieldsUtil.getFieldValue(str,
				// FieldsIndex.REVIEWER_NAME);
				String category = FieldsUtil.getFieldValue(str, FieldsIndex.CATEGORY);
				// String brand = FieldsUtil.getFieldValue(str,
				// FieldsIndex.BRAND);
				String overall = FieldsUtil.getFieldValue(str, FieldsIndex.OVERALL_RATING);
				String helpfulYes = FieldsUtil.getFieldValue(str, FieldsIndex.HELPFULLNESS_YES);
				String helpfulCount = FieldsUtil.getFieldValue(str, FieldsIndex.HELPFULLNESS_COUNT);

				t.append(category);
				t.append(Float.parseFloat(overall));
				t.append(Float.parseFloat(helpfulYes));
				t.append(Float.parseFloat(helpfulCount));
				return t;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Schema outputSchema(Schema input) {
		Schema tupleSchema = new Schema();
		try {
			tupleSchema.add(new FieldSchema("category", DataType.CHARARRAY));
			tupleSchema.add(new FieldSchema("overall", DataType.FLOAT));
			tupleSchema.add(new FieldSchema("helpfulYes", DataType.FLOAT));
			tupleSchema.add(new FieldSchema("helpfulCount", DataType.FLOAT));
			return new Schema(new FieldSchema(null, tupleSchema, DataType.TUPLE));
		} catch (FrontendException e) {
			e.printStackTrace();
			return null;
		}
	}
}
