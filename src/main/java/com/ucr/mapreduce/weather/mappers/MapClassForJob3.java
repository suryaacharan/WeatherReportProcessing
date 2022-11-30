package com.ucr.mapreduce.weather.mappers;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.ucr.mapreduce.weather.utils.MapReduceUtils;

public class MapClassForJob3 extends Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		private Text values = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer itr = new StringTokenizer(line, "\n");
			while (itr.hasMoreTokens()) {
				String str = itr.nextToken();
				String[] keyAttributes = str.split("_");
				String[] attr = keyAttributes[1].split("\\s+");
				String state = keyAttributes[0];
				String month = attr[0];
				String avg = attr[1];
				String prec = attr[2];
				if (MapReduceUtils.stringIsNotBlank(state) && MapReduceUtils.stringIsNotBlank(month)
						&& MapReduceUtils.stringIsNotBlank(avg)) {
					word.set(state);
					values.set(month.concat("_").concat(avg).concat("_").concat(prec));
					context.write(word, values);
				}
			}
		}
	}