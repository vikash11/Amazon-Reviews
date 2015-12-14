package util;

public class FieldsUtil {

	public static boolean isValid(String str) {
		String[] splitted = str.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
		return (splitted.length == FieldsIndex.LENGTH);
	}

	public static String getFieldValue(String str, int field) {
		String[] splitted = str.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
		if (splitted.length == FieldsIndex.LENGTH) {
			return splitted[field];
		} else {
			return "";
		}
	}
}
