package com.esri.hadoop.examples;
// adapted from Hadoop:_The_Definitive_Guide as well as existing earthquake aggregation sample

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.SequenceFile.Writer;

public class WriteSequenceFile {

    private static final String[] initial = {"one", "two", "three"};
    private static final String[] appendage = {"four", "five"};

	public static void main(String[] args) throws Exception {
		Configuration config = new Configuration();
		String uri = args[0];
		FileSystem hdfs = FileSystem.get(URI.create(uri), config);
		Path path = new Path(uri);

		IntWritable key = new IntWritable();
		Text value = new Text();
		SequenceFile.Writer writer = null;
		try {
			writer = SequenceFile.createWriter(hdfs, config, path,
											   IntWritable.class, Text.class);
			for (int ix = 0; ix < initial.length; ix++) {
				key.set(ix+1);
				value.set(initial[ix]);
				writer.append(key, value);
			}
		} finally {
			if (writer != null)
				writer.close();
		}
		// Truncates - does not append
		try {
			writer = new SequenceFile.Writer(hdfs, config, path,
											 IntWritable.class, Text.class);
			for (int ix = 0; ix < appendage.length; ix++) {
				key.set(ix+1);
				value.set(appendage[ix]);
				writer.append(key, value);
			}
		} finally {
			if (writer != null)
				writer.close();
		}
	}

}
