package com.ucr.mapreduce.weather.utils;

import static com.ucr.mapreduce.weather.utils.MapReduceConstants.UNDEF;

import java.util.Map.Entry;

import org.apache.log4j.Logger;

public class MapReduceUtils {
	private static Logger logger = Logger.getLogger(MapReduceUtils.class);
	public static float MIN_LATITUDE = Float.valueOf("-90.0000");
	public static float MAX_LATITUDE = Float.valueOf("90.0000");
	public static float MIN_LONGITUDE = Float.valueOf("-180.0000");
	public static float MAX_LONGITUDE = Float.valueOf("180.0000");

	public static boolean isValidCoordinates(String lat, String lon) {
		try {
			double latitude = Double.valueOf(lat);
			double longitude = Double.valueOf(lon);
			if (latitude >= MIN_LATITUDE && latitude <= MAX_LATITUDE && longitude >= MIN_LONGITUDE
					&& longitude <= MAX_LONGITUDE) {
				return true;
			}
		} catch (Exception e) {
			logger.info("Invalid Lat Long format");
			return false;
		}
		return false;

	}

	public static boolean stringequalsIgnoreCase(String a, String b) {
		if (a == null || b == null)
			return false;
		else if (a.equalsIgnoreCase(b))
			return true;
		else
			return false;
	}

	public static boolean stringIsNotBlank(String a) {
		if (a == null || a.trim().length() == 0 || a.length() < 1)
			return false;
		if (a.length() > 0)
			return true;
		else
			return false;
	}

	public static String getContentBwDoubleQuotes(String input) {
		String result = "";
		if (input != null && input.length() >= 2 && input.charAt(0) == '\"'
				&& input.charAt(input.length() - 1) == '\"') {
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

	// to:do handle Null
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

	public static String getClosestStateToGivenStation(StateCoordinates loc) {
		String state = UNDEF;
		Double least = Double.NaN;
		double distance;
		MapReduceConstants.stateCoordinatesMap.forEach((key, value) -> distanceBetweenTwoPointsInMiles(loc, value));
		for (Entry<String, StateCoordinates> entry : MapReduceConstants.stateCoordinatesMap.entrySet()) {
			distance = distanceBetweenTwoPointsInMiles(loc, entry.getValue());
			if (distance < 460) {
				if (Double.isNaN(least)) {
					least = distance;
					state = entry.getKey();
				} else if (distance < least) {
					least = distance;
					state = entry.getKey();
				}
			}
		}
		if(!MapReduceUtils.stringequalsIgnoreCase(state, UNDEF))
			logger.info("Nearest State "+state+" found for "+"lat "+loc.latitude+" long "+loc.longitude+" with distance of "+ least+" miles");
		return state;
	}

	public static double distanceBetweenTwoPointsInMiles(StateCoordinates loc1, StateCoordinates loc2) {
		double lat1 = loc1.latitude;
		double lon1 = loc1.longitude;
		double lat2 = loc2.latitude;
		double lon2 = loc2.longitude;
		if ((lat1 == lat2) && (lon1 == lon2)) {
			return 0;
		} else {
			double theta = lon1 - lon2;
			double dist = Math.sin(Math.toRadians(lat1)) * Math.sin(Math.toRadians(lat2))
					+ Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.cos(Math.toRadians(theta));
			dist = Math.acos(dist);
			dist = Math.toDegrees(dist);
			dist = dist * 60 * 1.1515;
			return dist;
		}
	}

	public static String getMonthNameFromNum(String monthNum) {
		String monthName = "";
		switch (monthNum) {
		case "01":
			monthName = "Jan";
			break;
		case "02":
			monthName = "Feb";
			break;
		case "03":
			monthName = "Mar";
			break;
		case "04":
			monthName = "Apr";
			break;
		case "05":
			monthName = "May";
			break;
		case "06":
			monthName = "Jun";
			break;
		case "07":
			monthName = "Jul";
			break;
		case "08":
			monthName = "Aug";
			break;
		case "09":
			monthName = "Sep";
			break;
		case "10":
			monthName = "Oct";
			break;
		case "11":
			monthName = "Nov";
			break;
		case "12":
			monthName = "Dec";
			break;

		default:
			break;
		}
		return monthName;
	}
}
