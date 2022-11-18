package com.ucr.mapreduce.weather;

import org.apache.log4j.Logger;

public class WeatherStationRecordParser {
	private static Logger logger = Logger.getLogger(WeatherStationRecordParser.class);

	private String countryName;
	private String stateName;
	private String usafID;

	public void parse(String record) {

		String[] attributes = record.split(",");
		if (attributes.length == 10 && MapReduceUtils.stringequalsIgnoreCase(attributes[3], "\"US\"")) {
			logger.info("Record found with valid USAF id in weather stations file");
			String country = MapReduceUtils.getContentBwDoubleQuotes(attributes[3]);
			String state = MapReduceUtils.getContentBwDoubleQuotes(attributes[4]);
			String usafNumber ="";
			if (MapReduceUtils.stringIsNotBlank(country) && MapReduceUtils.stringIsNotBlank(state)) {
				countryName = country;
				stateName = state;
				usafNumber = MapReduceUtils.getContentBwDoubleQuotes(attributes[0]);
				if(MapReduceUtils.stringIsNotBlank(usafNumber))
					usafID=usafNumber;
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
