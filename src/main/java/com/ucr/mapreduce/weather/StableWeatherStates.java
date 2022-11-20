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


public class StableWeatherStates extends Configured implements Tool {
	private static Logger logger = Logger.getLogger(StableWeatherStates.class);
	final long DEFAULT_SPLIT_SIZE = 128 * 1024 * 1024;
	
	//Input - CSV file
	//Output - key<USAF ID> value<State Name>
	//Filters US Stations by country
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

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
	
	//Input - txt files with weather data
	//Output - key<STATE_Month> value<usafID_Avg Temp*NoOfReadings>
	//Filters US Stations by country
	public static class MapClassForJob2 extends Mapper<LongWritable, Text, Text, Text> {

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
						&& MapReduceUtils.stringIsNotBlank(parser.getReadingCount())) {
					key_out = stationData.get(parser.getUsafID()).concat("_").concat(parser.getMonth());
					value_str = parser.getUsafID().concat("_")
							.concat(parser.getTemperature().concat("*").concat(parser.getReadingCount()));
				}
				if (MapReduceUtils.stringIsNotBlank(value_str) && MapReduceUtils.stringIsNotBlank(key_out)) {
					word.set(key_out);
					values.set(value_str);
					context.write(word, values);
				}
			}

		}
	}
	//input key<STATE_Month> monthly avg
	//Output key<State> value<month_avg>
	public static class MapClassForJob3 extends Mapper<LongWritable, Text, Text, Text> {
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
				if (MapReduceUtils.stringIsNotBlank(state) && MapReduceUtils.stringIsNotBlank(month)
						&& MapReduceUtils.stringIsNotBlank(avg)) {
					word.set(state);
					values.set(month.concat("_").concat(avg));
					context.write(word, values);
				}
			}
		}
	}

	//Use of combiner for avg
	//Input from MapJob2 key<STATE_Month> value<usafID_Avg Temp*NoOfReadings>
	//Output key<STATE_Month>  value<avg*count>
	
	public static class CombinerForJob2 extends Reducer<Text, Text, Text, Text> {
		  @Override
		  protected void reduce(final Text key, final Iterable<Text> values,
		    final Context context) throws IOException, InterruptedException {
		   Integer count = 0;
		   Double sum = 0D;
		   final Iterator<Text> itr = values.iterator();
		   while (itr.hasNext()) {
		    final String text = itr.next().toString();
		    String substr = text.substring(text.lastIndexOf("_")+1);
		    String[] tokens = substr.split("\\*");
		  
		    Double value = MapReduceUtils.getDoubleFromString(tokens[0]);
		    Integer ct = MapReduceUtils.getIntegerFromString(tokens[1]);
		    
		    if(value==Double.NaN)
		    	continue;
		    count+=ct;
		    sum += value*ct;
		    logger.info("tix : "+value.toString()+" "+ct.toString());
		    
		   }
		   
		   final Double average = sum / count;
		   logger.info("sum : "+sum.toString()+" "+count.toString());

		   context.write(key, new Text(average + "*" + count));
		  };
	}
	
	//Input key<STATE_Month>  value<avg*count>
	//Output key<STATE_Month> monthly avg
	public static class ReduceForJob2 extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			//String value_out = "";
			//for(Text val: values)
			//	value_out = value_out.concat(val.toString()).concat(" ");
			//value_out_text.set(value_out);
			//context.write(key, value_out_text);
			   Double sum = 0D;
			   Integer totalcount = 0;
			   final Iterator<Text> itr = values.iterator();
			   while (itr.hasNext()) {
			    final String text = itr.next().toString();
			    final String[] tokens = text.split("\\*");
			    
			    final Double average = MapReduceUtils.getDoubleFromString(tokens[0]);
			    final Integer count = MapReduceUtils.getIntegerFromString(tokens[1]);
			    sum += (average * count);
			    totalcount += count;
			   }

			   final Double average = sum / totalcount;
			   context.write(key, new Text(average.toString()));
		}
	}
	
	//Input(after shuffle and sort) State   Month1_Avg1 Month2_Avg2
	//Ouput key<State> <Max> <Min> <Difference>
	public static class ReducerForJob3 extends Reducer<Text, Text, Text, Text> {
		
		TreeMap<Double, String> sortedMap = new TreeMap<Double, String>();
		@Override
		protected void setup(Context context) throws IOException {
			try {
				context.write(new Text("State"), new Text("Maximum Minimum Difference"));
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
			   final Iterator<Text> itr = values.iterator();
			   while (itr.hasNext()) {
			    String month_Avg = itr.next().toString();
			    logger.info("rox : "+month_Avg);
			    Double value = MapReduceUtils.getDoubleFromString(month_Avg.substring(month_Avg.lastIndexOf("_")+1));
			    if(max==null)
			    	max=value;
			    if(min==null)
			    	min=value;
			    if(max!=null && value>max)
			    	max=value;
			    if(min!=null && value<min)
			    	min=value;
			   }

			   difference = max-min;
			   sortedMap.put(difference, key.toString().concat("_").concat(max.toString()).concat("_").concat(min.toString()));
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (Entry<Double, String> entry : sortedMap.entrySet()) {
				String[] vec = entry.getValue().split("_");
				logger.info("Final Tree map sort: " + vec[0]+ " "+ vec[1]+ " "+ vec[2]+ " "+entry.getKey().toString()) ;
				context.write(new Text(vec[0]),
						new Text(vec[1].concat(" ").concat(vec[2]).concat(" ").concat(entry.getKey().toString())));
			}
		}
	}
	
	//Input(after shuffle and sort) State Diff_Max_Min
	/*	public static class ReducerForJob4 extends Reducer<DoubleWritable, Text, Text, Text> {
			private Text values = new Text();
			@Override
			protected void setup(Context context) throws IOException {
				try {
					context.write(new Text("State"), new Text("Difference Maximum Minimum"));
				} catch (IOException e) {
					logger.info("IOException at ReducerForJob4");
				} catch (InterruptedException e) {
					logger.info("InterruptedException at ReducerForJob4");
				}

			}

			@Override
			protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
				   
				   final Iterator<Text> itr = values.iterator();
				   while (itr.hasNext()) {
				    String vect = itr.next().toString();
				    logger.info("rox1 : "+vect);
				    String[] elem = vect.split("_");
				    values
				    
				   }

				   context.write(new Text(difference.toString()), new Text(key.toString().concat("_").concat(max.toString()).concat("_").concat(min.toString())));
			}
		}
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
		//job2.setMapperClass(MapClassForJob2.class);
		job2.setCombinerClass(CombinerForJob2.class);
		job2.setReducerClass(ReduceForJob2.class);

		job.waitForCompletion(true);
		//Passing the output of job1 to job2 as distributed cache
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
		//Getting all text files in input directory
		FileStatus[] annualWeatherFileList = fs.listStatus(new Path(other_args.get(1)), new PathFilter() {
					@Override
					public boolean accept(Path path) {
						return path.getName().endsWith(".txt");
					}
				});
		for (int i = 0; i < annualWeatherFileList.length; i++) {
			logger.info("Count of txt dataset "+annualWeatherFileList.length);
			logger.info("Adding the txt file as input: "+annualWeatherFileList[i].getPath());
			MultipleInputs.addInputPath(job2, annualWeatherFileList[i].getPath(), TextInputFormat.class, MapClassForJob2.class);		    
		}
		//FileInputFormat.addInputPath(job2, new Path(other_args.get(2)));
		FileOutputFormat.setOutputPath(job2, new Path(other_args.get(3)));
		
		
		job2.waitForCompletion(true);
		
		Job job3 = Job.getInstance(config, "StableWeatherStates-3");

		job3.setJobName("MinMaxTemp");
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		job3.setJarByClass(StableWeatherStates.class);
		job3.setMapperClass(MapClassForJob3.class);
		job3.setReducerClass(ReducerForJob3.class);

		//Passing the output of job2 to job3 as Input
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
