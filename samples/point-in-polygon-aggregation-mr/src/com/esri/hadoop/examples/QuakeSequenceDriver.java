package com.esri.hadoop.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class QuakeSequenceDriver
{
	public static int main(String[] init_args) throws Exception {		
		Configuration config = new Configuration();
		
		// This step is important as init_args contains ALL the arguments passed to hadoop on the command
		// line (such as -libjars [jar files]).  What's left after .getRemainingArgs is just the arguments
		// intended for the MapReduce job
		String [] args = new GenericOptionsParser(config, init_args).getRemainingArgs();
		
		/*
		 * Args
		 *  [0] path(s) to the input data source
		 *  [1] path to write the output of the MapReduce jobs
		 */
		if (args.length != 2)
		{
			System.out.println("Invalid Arguments");
			print_usage();
			throw new IllegalArgumentException();
		}
		
		config.setInt("samples.csvdata.columns.lat", 1);
		config.setInt("samples.csvdata.columns.long", 2);
		
		Job job = new Job(config);

		job.setJobName("Earthquake Data Aggregation Sample");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(QuakeWritable.class);
		
		job.setMapperClass(QuakeSequenceMapper.class);
		//job.setReducerClass(ReducerClass.class);
		//job.setCombinerClass(ReducerClass.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		TextInputFormat.setInputPaths(job, new Path(args[0]));
		SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(QuakeSequenceDriver.class);

		return job.waitForCompletion(true)?  0 : 1;
	}
	
	static void print_usage()
	{
		System.out.println("***");
		System.out.println("Usage: hadoop jar aggregation-sample.jar QuakeSequenceDriver -libjars [external jar references] [/hdfs/path/to]/earthquakes.csv [/hdfs/path/to/user]/output.out");
		System.out.println("***");
	}
}
