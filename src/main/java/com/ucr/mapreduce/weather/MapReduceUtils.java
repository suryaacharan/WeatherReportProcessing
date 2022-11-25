package com.ucr.mapreduce.weather;

import org.apache.log4j.Logger;

public class MapReduceUtils {
	private static Logger logger = Logger.getLogger(MapReduceUtils.class);

	public static boolean stringequalsIgnoreCase(String a, String b) {
		if (a == null || b == null)
			return false;
		else if (a.equalsIgnoreCase(b))
			return true;
		else
			return false;
	}

	public static boolean stringIsNotBlank(String a) {
		if (a == null || a.trim().length() == 0|| a.length()<1)
			return false;
		if (a.length() > 0)
			return true;
		else
			return false;
	}
	
	public static String getContentBwDoubleQuotes(String input) {
		String result ="";
    	if (input != null && input.length() >= 2 
			      && input.charAt(0) == '\"' && input.charAt(input.length() - 1) == '\"') {
			    result = input.substring(1, input.length() - 1);
    	}
    	return result;
	}
	
	public static Double getDoubleFromString(String input) {
		Double d;
		try {
			d = Double.parseDouble(input);
		} catch (Exception e) {
			logger.error("Could not parse Double from String for average calculation");
			return Double.NaN;
		}
		return d;
	}
	
	//to:do handle Null
	public static Integer getIntegerFromString(String input) {
		Integer i;
		try {
			i = Integer.parseInt(input);
		} catch (Exception e) {
			logger.error("Could not parse Integer from String for average calculation");
			return null;
		}
		return i;
	}
	
	public static String getMonthNameFromNum(String monthNum) {
		String monthName = "";
		switch (monthNum) {
		case "01":
			monthName="Jan";
			break;
		case "02":
			monthName="Feb";
			break;
		case "03":
			monthName="Mar";
			break;	
		case "04":
			monthName="Apr";
			break;
		case "05":
			monthName="May";
			break;
		case "06":
			monthName="Jun";
			break;
		case "07":
			monthName="Jul";
			break;
		case "08":
			monthName="Aug";
			break;
		case "09":
			monthName="Sep";
			break;
		case "10":
			monthName="Oct";
			break;
		case "11":
			monthName="Nov";
			break;
		case "12":
			monthName="Dec";
			break;

		default:
			break;
		}
		return monthName;
	}
}
