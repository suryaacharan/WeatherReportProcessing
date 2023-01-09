package com.ucr.mapreduce.weather.parsers;

import org.apache.log4j.Logger;

import com.ucr.mapreduce.weather.utils.MapReduceConstants;
import com.ucr.mapreduce.weather.utils.MapReduceUtils;
import com.ucr.mapreduce.weather.utils.StateCoordinates;

public class WeatherStationRecordParser {
	private static Logger logger = Logger.getLogger(WeatherStationRecordParser.class);

	private String countryName;
	private String stateName;
	private String usafID;

	public void parse(String record, boolean include_approx) {

		String[] attributes = record.split(",");
		if (attributes.length == 10 && MapReduceUtils.stringequalsIgnoreCase(attributes[3], "\"US\"")) {
			logger.info("Record found with valid USAF id in weather stations file");
			String country = MapReduceUtils.getContentBwDoubleQuotes(attributes[3]);
			String state = MapReduceUtils.getContentBwDoubleQuotes(attributes[4]);
			String usafNumber = MapReduceUtils.getContentBwDoubleQuotes(attributes[0]);
			if (MapReduceUtils.stringIsNotBlank(state)) {
				countryName = country;
				stateName = state;
				
				if (MapReduceUtils.stringIsNotBlank(usafNumber))
					usafID = usafNumber;
			} else if(include_approx){
				String stnName = MapReduceUtils.getContentBwDoubleQuotes(attributes[2]);
				if (stnName != null && stnName.contains(MapReduceConstants.BUOY)) {
					logger.info("Ocean Buoy "+ usafNumber+"found in "+country);
					countryName = country;
					stateName = MapReduceConstants.BUOY;
					usafID = usafNumber;
				} else {
					logger.info("Station "+ usafNumber+" found in "+country+ " without state- Attempting to find closest state in USA");
					String latString = MapReduceUtils.getContentBwDoubleQuotes(attributes[5]);
					String lonString = MapReduceUtils.getContentBwDoubleQuotes(attributes[6]);
					if (MapReduceUtils.isValidCoordinates(latString, lonString)) {
						countryName = country;
						stateName = MapReduceUtils.getClosestStateToGivenStation(
								new StateCoordinates(Double.valueOf(latString), Double.valueOf(lonString)));
						usafID = usafNumber;
					}

				}

			}

		}
	}

	public String getCountryName() {
		return countryName;
	}

	public String getStateName() {
		return stateName;
	}

	public String getUsafID() {
		return usafID;
	}
}
