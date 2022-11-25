package com.ucr.mapreduce.weather.mappers;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.ucr.mapreduce.weather.MapReduceUtils;

public class MapClassForJob4 extends Mapper<LongWritable, Text, DoubleWritable, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer itr = new StringTokenizer(line, "\n");
			while (itr.hasMoreTokens()) {
				String str = itr.nextToken();
				String[] attr = str.split("\\s+");
				String diff = attr[0];
				String vec = attr[1];
				if (MapReduceUtils.stringIsNotBlank(diff) && MapReduceUtils.stringIsNotBlank(vec)) {

					context.write(new DoubleWritable(MapReduceUtils.getDoubleFromString(diff)), new Text(vec));
				}
			}
		}
	}