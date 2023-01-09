package com.ucr.mapreduce.weather.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
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
import com.ucr.mapreduce.weather.utils.MapReduceConstants;

public class StableWeatherStatesJobRunner extends Configured implements Tool {
	private static Logger logger = Logger.getLogger(StableWeatherStatesJobRunner.class);
	final long DEFAULT_SPLIT_SIZE = 128 * 1024 * 1024;

	static int printUsage() {
		System.out.println("hadoop jar <pathToJar> <pathToWeatherStationLocationsinHDFS> <pathToDirectoryOfTxtFiles> <outputDirForJob1> <outputDirForJob2> <outputDirForJob3> | optional -include_approx");
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
		if(args.length<5)
			printUsage();
		
		if(args.length>5 && args[5].contains(MapReduceConstants.INCLUDE_APPROX))
			config.set(MapReduceConstants.INCLUDE_APPROX, MapReduceConstants.INCLUDE_APPROX);
		Job job = Job.getInstance(config, "StableWeatherStatesJobRunner");
		job.setJobName("Get US StableWeatherStatesJobRunner Stations with State");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setJarByClass(StableWeatherStatesJobRunner.class);
		job.setMapperClass(MapClass.class);

		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		Job job2 = Job.getInstance(config, "StableWeatherStatesJobRunner-2");
		job2.setJobName("MonthlyAverage");
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setJarByClass(StableWeatherStatesJobRunner.class);
		job2.setCombinerClass(CombinerForJob2.class);
		job2.setReducerClass(ReduceForJob2.class);
		
		job.waitForCompletion(true);
		
		// Passing the output of job1 to job2 as distributed cache
		FileSystem fs = FileSystem.get(config);
		FileStatus[] fileList = fs.listStatus(new Path(args[2]), new PathFilter() {
			@Override
			public boolean accept(Path path) {
				return path.getName().startsWith("part-");
			}
		});
		
		for (int i = 0; i < fileList.length; i++) {
			job2.addCacheFile(fileList[i].getPath().toUri());
		}
		
		// Getting all text files in input directory
		FileStatus[] annualWeatherFileList = fs.listStatus(new Path(args[1]), new PathFilter() {
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
		FileOutputFormat.setOutputPath(job2, new Path(args[3]));
		job2.waitForCompletion(true);

		Job job3 = Job.getInstance(config, "StableWeatherStatesJobRunner-3");
		job3.setJobName("MinMaxTemp");
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		job3.setJarByClass(StableWeatherStatesJobRunner.class);
		job3.setMapperClass(MapClassForJob3.class);
		job3.setReducerClass(ReducerForJob3.class);

		// Passing the output of job2 to job3 as Input
		fileList = fs.listStatus(new Path(args[3]), new PathFilter() {
			@Override
			public boolean accept(Path path) {
				return path.getName().startsWith("part-");
			}
		});
		
		for (int i = 0; i < fileList.length; i++) {
			FileInputFormat.addInputPath(job3, fileList[i].getPath());
		}

		FileOutputFormat.setOutputPath(job3, new Path(args[4]));
		job3.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new StableWeatherStatesJobRunner(), args);
		System.exit(res);
	}

}
