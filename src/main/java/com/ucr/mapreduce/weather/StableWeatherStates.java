package com.ucr.mapreduce.weather;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.ucr.mapreduce.weather.mappers.MapClass;
import com.ucr.mapreduce.weather.mappers.MapClassForJob2;
import com.ucr.mapreduce.weather.mappers.MapClassForJob3;
import com.ucr.mapreduce.weather.reducers.CombinerForJob2;
import com.ucr.mapreduce.weather.reducers.ReduceForJob2;
import com.ucr.mapreduce.weather.reducers.ReducerForJob3;

public class StableWeatherStates extends Configured implements Tool {
	private static Logger logger = Logger.getLogger(StableWeatherStates.class);
	final long DEFAULT_SPLIT_SIZE = 128 * 1024 * 1024;

	/*
	 * public static class MapClassForJob4 extends Mapper<LongWritable, Text,
	 * DoubleWritable, Text> {
	 * 
	 * public void map(LongWritable key, Text value, Context context) throws
	 * IOException, InterruptedException { String line = value.toString();
	 * StringTokenizer itr = new StringTokenizer(line, "\n"); while
	 * (itr.hasMoreTokens()) { String str = itr.nextToken(); String[] attr =
	 * str.split("\\s+"); String diff = attr[0]; String vec = attr[1]; if
	 * (MapReduceUtils.stringIsNotBlank(diff) &&
	 * MapReduceUtils.stringIsNotBlank(vec)) {
	 * 
	 * context.write(new DoubleWritable(MapReduceUtils.getDoubleFromString(diff)),
	 * new Text(vec)); } } } }
	 */

	// Input(after shuffle and sort) State Diff_Max_Min
	/*
	 * public static class ReducerForJob4 extends Reducer<DoubleWritable, Text,
	 * Text, Text> { private Text values = new Text();
	 * 
	 * @Override protected void setup(Context context) throws IOException { try {
	 * context.write(new Text("State"), new Text("Difference Maximum Minimum")); }
	 * catch (IOException e) { logger.info("IOException at ReducerForJob4"); } catch
	 * (InterruptedException e) {
	 * logger.info("InterruptedException at ReducerForJob4"); }
	 * 
	 * }
	 * 
	 * @Override protected void reduce(DoubleWritable key, Iterable<Text> values,
	 * Context context) throws IOException, InterruptedException {
	 * 
	 * final Iterator<Text> itr = values.iterator(); while (itr.hasNext()) { String
	 * vect = itr.next().toString(); logger.info("rox1 : "+vect); String[] elem =
	 * vect.split("_"); values
	 * 
	 * }
	 * 
	 * context.write(new Text(difference.toString()), new
	 * Text(key.toString().concat("_").concat(max.toString()).concat("_").concat(min
	 * .toString()))); } }
	 */

	static int printUsage() {
		System.out.println("weather [-m <maps>] [-r <reduces>] <job_1 input> <job_1 output> <job_2 input>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * The main driver for weather map/reduce program. Invoke this method to submit
	 * the map/reduce job.
	 * 
	 * @throws IOException When there is communication problems with the job
	 *                     tracker.
	 */
	public int run(String[] args) throws Exception {
		Configuration config = getConf();

		Job job = Job.getInstance(config, "StableWeatherStates");

		job.setJobName("Get US StableWeatherStates Stations with State");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setJarByClass(StableWeatherStates.class);
		job.setMapperClass(MapClass.class);

		List<String> other_args = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			try {
				if ("-m".equals(args[i])) {
					// job.setNumMapTasks(Integer.parseInt(args[++i]));
				} else if ("-r".equals(args[i])) {
					job.setNumReduceTasks(Integer.parseInt(args[++i]));
				} else {
					other_args.add(args[i]);
				}
			} catch (NumberFormatException except) {
				System.out.println("ERROR: Integer expected instead of " + args[i]);
				return printUsage();
			} catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from " + args[i - 1]);
				return printUsage();
			}
		}

		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path(other_args.get(0)));
		FileOutputFormat.setOutputPath(job, new Path(other_args.get(2)));

		Job job2 = Job.getInstance(config, "StableWeatherStates-2");

		job2.setJobName("MonthlyAverage");
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setJarByClass(StableWeatherStates.class);
		// job2.setMapperClass(MapClassForJob2.class);
		job2.setCombinerClass(CombinerForJob2.class);
		job2.setReducerClass(ReduceForJob2.class);
		//job2.setNumReduceTasks(0);
		job.waitForCompletion(true);
		// Passing the output of job1 to job2 as distributed cache
		FileSystem fs = FileSystem.get(config);
		FileStatus[] fileList = fs.listStatus(new Path(other_args.get(2)), new PathFilter() {
			@Override
			public boolean accept(Path path) {
				return path.getName().startsWith("part-");
			}
		});
		for (int i = 0; i < fileList.length; i++) {
			job2.addCacheFile(fileList[i].getPath().toUri());
		}
		// Getting all text files in input directory
		FileStatus[] annualWeatherFileList = fs.listStatus(new Path(other_args.get(1)), new PathFilter() {
			@Override
			public boolean accept(Path path) {
				return path.getName().endsWith(".txt");
			}
		});
		for (int i = 0; i < annualWeatherFileList.length; i++) {
			logger.info("Count of txt dataset " + annualWeatherFileList.length);
			logger.info("Adding the txt file as input: " + annualWeatherFileList[i].getPath());
			MultipleInputs.addInputPath(job2, annualWeatherFileList[i].getPath(), TextInputFormat.class,
					MapClassForJob2.class);
		}
		// FileInputFormat.addInputPath(job2, new Path(other_args.get(2)));
		FileOutputFormat.setOutputPath(job2, new Path(other_args.get(3)));

		job2.waitForCompletion(true);

		Job job3 = Job.getInstance(config, "StableWeatherStates-3");

		job3.setJobName("MinMaxTemp");
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		job3.setJarByClass(StableWeatherStates.class);
		job3.setMapperClass(MapClassForJob3.class);
		job3.setReducerClass(ReducerForJob3.class);

		// Passing the output of job2 to job3 as Input
		fileList = fs.listStatus(new Path(other_args.get(3)), new PathFilter() {
			@Override
			public boolean accept(Path path) {
				return path.getName().startsWith("part-");
			}
		});
		for (int i = 0; i < fileList.length; i++) {
			FileInputFormat.addInputPath(job3, fileList[i].getPath());
		}

		FileOutputFormat.setOutputPath(job3, new Path(other_args.get(4)));

		job3.waitForCompletion(true);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new StableWeatherStates(), args);
		System.exit(res);
	}

}
