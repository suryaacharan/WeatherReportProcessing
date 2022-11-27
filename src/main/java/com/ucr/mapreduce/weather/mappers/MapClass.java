package com.ucr.mapreduce.weather.mappers;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.ucr.mapreduce.weather.MapReduceUtils;
import com.ucr.mapreduce.weather.WeatherStationRecordParser;

//Input - CSV file
//Output - key<USAF ID> value<State Name>
//Filters US Stations by country
public class MapClass extends Mapper<LongWritable, Text, Text, Text> {

	private Text word = new Text();
	private Text values = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		WeatherStationRecordParser parser = new WeatherStationRecordParser();
		String key_out = null;
		String value_str = null;
		String line = value.toString();
		StringTokenizer itr = new StringTokenizer(line, "\n");
		while (itr.hasMoreTokens()) {
			String str = itr.nextToken();
			parser.parse(str);
			if (MapReduceUtils.stringIsNotBlank(parser.getUsafID())
					&& MapReduceUtils.stringIsNotBlank(parser.getCountryName())
					&& MapReduceUtils.stringIsNotBlank(parser.getStateName())) {
				key_out = parser.getUsafID();
				value_str = parser.getStateName();
			}
			if (MapReduceUtils.stringIsNotBlank(value_str) && MapReduceUtils.stringIsNotBlank(key_out)) {
				word.set(key_out);
				values.set(value_str);
				context.write(word, values);
			}
		}

	}
}