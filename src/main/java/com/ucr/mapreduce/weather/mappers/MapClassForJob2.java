package com.ucr.mapreduce.weather.mappers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.ucr.mapreduce.weather.MapReduceUtils;
import com.ucr.mapreduce.weather.WeatherDetailsRecordParser;

//Input - txt files with weather data
//Output - key<STATE_Month> value<usafID_Avg Temp*NoOfReadings>
//Filters US Stations by country
public class MapClassForJob2 extends Mapper<LongWritable, Text, Text, Text> {

	private Text word = new Text();
	private Text values = new Text();
	HashMap<String, String> stationData = new HashMap<String, String>();
	
	@SuppressWarnings("deprecation")
	@Override
	protected void setup(Context context) throws IOException {
		try {
			Path[] fileCached = context.getLocalCacheFiles();
			if (fileCached != null && fileCached.length > 0) {
				for (Path file : fileCached) {
					readFile(file);
				}
			}
		} catch (IOException ex) {
			System.err.println("Exception in mapper setup: " + ex.getMessage());
		}
	}
	
	private void readFile(Path filePath) {
	    try{
	        BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));
	        String line = null;
	        while((line = bufferedReader.readLine()) != null) {
	            //reading line by line that file and updating our struct store
	        	String[] attributes = line.split("\\s+");
	        	stationData.put(attributes[0], attributes[1]);
	        } //end while (cycling over lines in file)
	        bufferedReader.close();
	    } catch(IOException ex) {
	        System.err.println("Exception while reading file: " + ex.getMessage());
	    }
	}

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		WeatherDetailsRecordParser parser = new WeatherDetailsRecordParser();
		String key_out = null;
		String value_str = null;
		String line = value.toString();
		StringTokenizer itr = new StringTokenizer(line, "\n");
		while (itr.hasMoreTokens()) {
			String str = itr.nextToken();
			if (str.contains("STN"))
				continue;
			parser.parse(str);
			if(!stationData.containsKey(parser.getUsafID()))
				continue;
			if (MapReduceUtils.stringIsNotBlank(parser.getUsafID())
					&& MapReduceUtils.stringIsNotBlank(parser.getMonth())
					&& MapReduceUtils.stringIsNotBlank(parser.getTemperature())
					&& MapReduceUtils.stringIsNotBlank(parser.getReadingCount())
					&& MapReduceUtils.stringIsNotBlank(parser.getPrecipitation())) {
				key_out = stationData.get(parser.getUsafID()).concat("_").concat(parser.getMonth());
				value_str = parser.getUsafID().concat("_")
						.concat(parser.getTemperature().concat("*").concat(parser.getReadingCount())).concat("_").concat(parser.getPrecipitation());
			}
			if (MapReduceUtils.stringIsNotBlank(value_str) && MapReduceUtils.stringIsNotBlank(key_out)) {
				word.set(key_out);
				values.set(value_str);
				context.write(word, values);
			}
		}

	}
}