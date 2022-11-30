package com.ucr.mapreduce.weather.reducers;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import com.ucr.mapreduce.weather.job.StableWeatherStatesJobRunner;
import com.ucr.mapreduce.weather.utils.MapReduceUtils;

//Use of combiner for avg
//Input from MapJob2 key<STATE_Month> value<usafID_Avg Temp*NoOfReadings>	
public class CombinerForJob2 extends Reducer<Text, Text, Text, Text> {
	private static Logger logger = Logger.getLogger(StableWeatherStatesJobRunner.class);
	@Override
	protected void reduce(final Text key, final Iterable<Text> values,
    final Context context) throws IOException, InterruptedException {
		Integer count = 0;
		Double sum = 0D;
		Double precSum = 0D;
		int precipCount=0;
		final Iterator<Text> itr = values.iterator();
		while (itr.hasNext()) {
			final String text = itr.next().toString();
		    //String substr = text.substring(text.lastIndexOf("_")+1);
		    String substr = text.substring(text.indexOf("_")+1,text.lastIndexOf("_"));
		    String precSubStr = text.substring(text.lastIndexOf("_")+1);
		    String[] tokens = substr.split("\\*");
		    Double value = MapReduceUtils.getDoubleFromString(tokens[0]);
		    Integer ct = MapReduceUtils.getIntegerFromString(tokens[1]);
		    Double precipitation = MapReduceUtils.getDoubleFromString(precSubStr);
		    if(value==Double.NaN)
		    	continue;
		    count+=ct;
		    sum += value*ct;
		    precSum+=precipitation;
		    precipCount++;
		    logger.info("tix : "+value.toString()+" "+ct.toString());
		    
		}
		final Double average = sum / count;
		final Double averagePrec = precSum / precipCount;
		logger.info("sum : "+sum.toString()+" "+count.toString());
		context.write(key, new Text(average + "_" + count + "_" + averagePrec + "_" + precipCount));
	};
}