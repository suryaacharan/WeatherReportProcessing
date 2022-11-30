package com.ucr.mapreduce.weather.reducers;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import com.ucr.mapreduce.weather.job.StableWeatherStatesJobRunner;
import com.ucr.mapreduce.weather.utils.MapReduceUtils;

//Input(after shuffle and sort) State   Month1_Avg1 Month2_Avg2
public class ReducerForJob3 extends Reducer<Text, Text, Text, Text> {
	private static Logger logger = Logger.getLogger(StableWeatherStatesJobRunner.class);
	TreeMap<Double, String> sortedMap = new TreeMap<Double, String>();

	@Override
	protected void setup(Context context) throws IOException {
		try {
			context.write(new Text("State"+"\t"), new Text("Maximum"+"\t"+"Precipitation"+"\t"+"Minimum"+"\t"+"Precipitation"+"\t"+"Difference"));
		} catch (IOException e) {
			logger.info("IOException at ReducerForJob4");
		} catch (InterruptedException e) {
			logger.info("InterruptedException at ReducerForJob4");
		}

	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Double max = null;
		Double min = null;
		Double difference = 0D;
		String monthValue = "";
		String maxMonth = null;
		String minMonth = null;
		Double maxPrecipitation = null;
		Double minPrecipitation = null;
		final Iterator<Text> itr = values.iterator();
		while (itr.hasNext()) {
			String month_Avg = itr.next().toString();
			logger.info("rox : " + month_Avg);
			monthValue = MapReduceUtils.getMonthNameFromNum(month_Avg.substring(0,month_Avg.indexOf("_")));
			Double value = BigDecimal.valueOf(MapReduceUtils.getDoubleFromString(month_Avg.substring(month_Avg.indexOf("_") + 1,month_Avg.lastIndexOf("_")))).setScale(2, RoundingMode.HALF_UP).doubleValue();
			Double precValue = BigDecimal.valueOf(MapReduceUtils.getDoubleFromString(month_Avg.substring(month_Avg.lastIndexOf("_")+1))).setScale(2,RoundingMode.HALF_UP).doubleValue();
			if (max == null) {
				max = value;
				maxMonth = monthValue;
				maxPrecipitation = precValue;
			}
			if (min == null) {
				min = value;
				minMonth = monthValue;
				minPrecipitation = precValue;
			}
			if (max != null && value > max) {
				max = value;
				maxMonth = monthValue;
				maxPrecipitation = precValue;
			}
			if (min != null && value < min) {
				min = value;
				minMonth = monthValue;
				minPrecipitation = precValue;
			}
		}
		
		difference = max - min;
		difference = BigDecimal.valueOf(difference).setScale(2, RoundingMode.HALF_UP).doubleValue();
		String val = key.toString().concat("_").concat(maxMonth).concat("_").concat(max.toString()).concat("_").concat(maxPrecipitation.toString()).concat("_").concat(minMonth).concat("_").concat(min.toString()).concat("_").concat(minPrecipitation.toString());
		sortedMap.put(difference, val );
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (Entry<Double, String> entry : sortedMap.entrySet()) {
			String[] vec = entry.getValue().split("_");
			logger.info(
					"Final Tree map sort: " + vec[0] + " " + vec[1] + " " + vec[2] + " " + entry.getKey().toString());
			context.write(new Text(vec[0]),
					new Text(vec[1].concat("\t").concat(vec[2]).concat("\t").concat(vec[3]).concat("\t").concat(vec[4]).concat("\t").concat(vec[5]).concat("\t").concat(vec[6]).concat("\t").concat(entry.getKey().toString())));
		}
	}
}