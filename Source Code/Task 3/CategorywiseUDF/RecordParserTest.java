package org.apache.pig.builtin;

import util.FieldsIndex;
import util.FieldsUtil;

public class RecordParserTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		displayStr("");
	}

	private static void displayStr(String str) {
		String asin = FieldsUtil.getFieldValue(str, FieldsIndex.ASIN);
		String reviewerID = FieldsUtil.getFieldValue(str, FieldsIndex.REVIEWER_ID);
		String reviewerName = FieldsUtil.getFieldValue(str, FieldsIndex.REVIEWER_NAME);
		String category = FieldsUtil.getFieldValue(str, FieldsIndex.CATEGORY);
		String brand = FieldsUtil.getFieldValue(str, FieldsIndex.BRAND);
		String overall = FieldsUtil.getFieldValue(str, FieldsIndex.OVERALL_RATING);
		String helpfulYes = FieldsUtil.getFieldValue(str, FieldsIndex.HELPFULLNESS_YES);
		String helpfulCount = FieldsUtil.getFieldValue(str, FieldsIndex.HELPFULLNESS_COUNT);

		System.out.println(category);
		System.out.println(Float.parseFloat(overall));
		System.out.println(Integer.parseInt(helpfulYes));
		System.out.println(Integer.parseInt(helpfulCount));
	}

}
