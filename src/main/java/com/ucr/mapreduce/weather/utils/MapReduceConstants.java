package com.ucr.mapreduce.weather.utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class MapReduceConstants {

	private MapReduceConstants() {
	}

	public static final String UNDEF = "undefined";
	
	public static final String BUOY = "BUOY";
	
	public static final String INCLUDE_APPROX = "include_approx";

	public static final Map<String, StateCoordinates> stateCoordinatesMap = initMap();

	private static Map<String, StateCoordinates> initMap() {
		Map<String, StateCoordinates> map = new HashMap<String, StateCoordinates>();
		map.put("AK", new StateCoordinates(63.588753, -154.493062));
		map.put("AL", new StateCoordinates(32.318231, -86.902298));
		map.put("AR", new StateCoordinates(35.20105, -91.831833));
		map.put("AZ", new StateCoordinates(34.048928, -111.093731));
		map.put("CA", new StateCoordinates(36.778261, -119.417932));
		map.put("CO", new StateCoordinates(39.550051, -105.782067));
		map.put("CT", new StateCoordinates(41.603221, -73.087749));
		map.put("DC", new StateCoordinates(38.905985, -77.033418));
		map.put("DE", new StateCoordinates(38.910832, -75.52767));
		map.put("FL", new StateCoordinates(27.664827, -81.515754));
		map.put("GA", new StateCoordinates(32.157435, -82.907123));
		map.put("HI", new StateCoordinates(19.898682, -155.665857));
		map.put("IA", new StateCoordinates(41.878003, -93.097702));
		map.put("ID", new StateCoordinates(44.068202, -114.742041));
		map.put("IL", new StateCoordinates(40.633125, -89.398528));
		map.put("IN", new StateCoordinates(40.551217, -85.602364));
		map.put("KS", new StateCoordinates(39.011902, -98.484246));
		map.put("KY", new StateCoordinates(37.839333, -84.270018));
		map.put("LA", new StateCoordinates(31.244823, -92.145024));
		map.put("MA", new StateCoordinates(42.407211, -71.382437));
		map.put("MD", new StateCoordinates(39.045755, -76.641271));
		map.put("ME", new StateCoordinates(45.253783, -69.445469));
		map.put("MI", new StateCoordinates(44.314844, -85.602364));
		map.put("MN", new StateCoordinates(46.729553, -94.6859));
		map.put("MO", new StateCoordinates(37.964253, -91.831833));
		map.put("MS", new StateCoordinates(32.354668, -89.398528));
		map.put("MT", new StateCoordinates(46.879682, -110.362566));
		map.put("NC", new StateCoordinates(35.759573, -79.0193));
		map.put("ND", new StateCoordinates(47.551493, -101.002012));
		map.put("NE", new StateCoordinates(41.492537, -99.901813));
		map.put("NH", new StateCoordinates(43.193852, -71.572395));
		map.put("NJ", new StateCoordinates(40.058324, -74.405661));
		map.put("NM", new StateCoordinates(34.97273, -105.032363));
		map.put("NV", new StateCoordinates(38.80261, -116.419389));
		map.put("NY", new StateCoordinates(43.299428, -74.217933));
		map.put("OH", new StateCoordinates(40.417287, -82.907123));
		map.put("OK", new StateCoordinates(35.007752, -97.092877));
		map.put("OR", new StateCoordinates(43.804133, -120.554201));
		map.put("PA", new StateCoordinates(41.203322, -77.194525));
		map.put("PR", new StateCoordinates(18.220833, -66.590149));
		map.put("RI", new StateCoordinates(41.580095, -71.477429));
		map.put("SC", new StateCoordinates(33.836081, -81.163725));
		map.put("SD", new StateCoordinates(43.969515, -99.901813));
		map.put("TN", new StateCoordinates(35.517491, -86.580447));
		map.put("TX", new StateCoordinates(31.968599, -99.901813));
		map.put("UT", new StateCoordinates(39.32098, -111.093731));
		map.put("VA", new StateCoordinates(37.431573, -78.656894));
		map.put("VT", new StateCoordinates(44.558803, -72.577841));
		map.put("WA", new StateCoordinates(47.751074, -120.740139));
		map.put("WI", new StateCoordinates(43.78444, -88.787868));
		map.put("WV", new StateCoordinates(38.597626, -80.454903));
		map.put("WY", new StateCoordinates(43.075968, -107.290284));
		return Collections.unmodifiableMap(map);
	}

}