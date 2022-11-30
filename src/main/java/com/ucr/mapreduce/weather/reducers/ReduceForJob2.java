package com.ucr.mapreduce.weather.reducers;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.ucr.mapreduce.weather.utils.MapReduceUtils;

public class ReduceForJob2 extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// String value_out = "";
		// for(Text val: values)
		// value_out = value_out.concat(val.toString()).concat(" ");
		// value_out_text.set(value_out);
		// context.write(key, value_out_text);
		Double sum = 0D;
		Double precSum = 0D;
		Integer totalcount = 0;
		Integer totalPrecCount = 0;
		final Iterator<Text> itr = values.iterator();
		while (itr.hasNext()) {
			final String text = itr.next().toString();
			final String[] bigTokens = text.split("_");
			//final String[] tempTokens = bigTokens[0].split("//*");
			//final String[] precTokens = bigTokens[1].split("//*");
			final Double average = MapReduceUtils.getDoubleFromString(bigTokens[0]);
			final Integer count = MapReduceUtils.getIntegerFromString(bigTokens[1]);
			final Double prec = MapReduceUtils.getDoubleFromString(bigTokens[2]);
			final Integer precCnt = MapReduceUtils.getIntegerFromString(bigTokens[3]);
			sum += (average * count);
			precSum += (prec * precCnt);
			totalcount += count;
			totalPrecCount += precCnt;
		}
		final Double average = sum / totalcount;
		final Double precAverage = precSum / totalPrecCount;
		context.write(key, new Text(average.toString().concat(" ").concat(precAverage.toString())));
	}
}