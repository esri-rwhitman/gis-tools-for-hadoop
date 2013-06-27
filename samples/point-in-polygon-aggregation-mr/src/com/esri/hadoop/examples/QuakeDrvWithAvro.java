package com.esri.hadoop.examples;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class QuakeDrvWithAvro
{
	public static void main(String[] init_args) throws Exception {		
		Configuration config = new Configuration();
		
		// This step is important as init_args contains ALL the arguments passed to hadoop on the command
		// line (such as -libjars [jar files]).  What's left after .getRemainingArgs is just the arguments
		// intended for the MapReduce job
		String [] args = new GenericOptionsParser(config, init_args).getRemainingArgs();
		
		/*
		 * Args
		 *  [0] path to Esri JSON file
		 *  [1] path(s) to the input data source
		 *  [2] path to write the output of the MapReduce jobs
		 */
		if (args.length != 3)
		{
			System.out.println("Invalid Arguments");
			print_usage();
			System.out.println("Arguments ~ " + args.length + ": " + args[0] + "|" + args[1]);
			throw new IllegalArgumentException();
		}
		
		config.set("sample.features.input", args[0]);
		config.set("sample.features.keyattribute", "NAME");

		Job job = new Job(config);

		job.setJobName("Earthquake Data Aggregation ~ Avro");

		job.setOutputKeyClass(AvroKey.class);
		job.setOutputValueClass(AvroValue.class);
		
		job.setMapperClass(QuakeMapWithAvro.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(ReducerClass.class);
		job.setCombinerClass(ReducerClass.class);

		job.setInputFormatClass(AvroKeyInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Trying to parse the equivalent schema from string,
		// would result in mismatch between Generic & Specific.
		AvroJob.setInputKeySchema(job, Earthquake.SCHEMA$);

		FileInputFormat.setInputPaths(job, new Path(args[1]));
		TextOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setJarByClass(QuakeDrvWithAvro.class);

		System.exit( job.waitForCompletion(true) ? 0 : 1 );
	}
	
	static void print_usage()
	{
		System.out.println("***");
		System.out.println("Usage: hadoop jar aggregation-sample.jar QuakeDrvWithAvro -libjars [external jar references] [/hdfs/path/to]/filtergeometry.json [/hdfs/path/to]/earthquakes.seq [/hdfs/path/to/user]/output.out");
		System.out.println("***");
	}
}
