package com.ucr.mapreduce.weather.parsers;

import java.io.Serializable;

import org.apache.log4j.Logger;

import com.ucr.mapreduce.weather.utils.MapReduceUtils;

public class WeatherDetailsRecordParser implements Serializable {
	private static Logger logger = Logger.getLogger(WeatherDetailsRecordParser.class);
	private static final long serialVersionUID = 1234567L;
	private String month;
	private String usafID;
	private String temperature;
	private String readingCount;
	private String precipitation;

	public void parse(String record) {
		// \s+ matches all the space characters (single or multiple)
		String[] attributes = record.split("\\s+");
		if (MapReduceUtils.stringIsNotBlank(attributes[0])) {
			logger.info("Record found with valid USAF id in weather details file " + attributes[0] + " " + attributes[2]
					+ " " + attributes[3]);
			String parsedStn = attributes[0];
			String parsedDate = attributes[2];
			String parsedTemperature = attributes[3];
			String rCount = attributes[4];
			String parsedPrecipitation = attributes[19];
			parsedPrecipitation = parsedPrecipitation.replaceAll("[^0-9.]","");
			this.usafID = parsedStn;
			if (MapReduceUtils.stringIsNotBlank(parsedDate) && parsedDate.length() == 8)
				this.month = parsedDate.substring(4, 6);
			if (MapReduceUtils.stringIsNotBlank(parsedTemperature))
				this.temperature = parsedTemperature;
			if (MapReduceUtils.stringIsNotBlank(parsedPrecipitation))
				this.precipitation = parsedPrecipitation;
			if (MapReduceUtils.stringIsNotBlank(rCount))
				this.readingCount = rCount;
			logger.info(
					"Record Found with valid USAF id in weather details file " + temperature + " " + month + " " + usafID+ " "+ readingCount);
		}
	}

	public String getPrecipitation() {
		return precipitation;
	}
	public String getTemperature() {
		return temperature;
	}

	public String getMonth() {
		return month;
	}

	public String getUsafID() {
		return usafID;
	}

	public String getReadingCount() {
		return readingCount;
	}

}
