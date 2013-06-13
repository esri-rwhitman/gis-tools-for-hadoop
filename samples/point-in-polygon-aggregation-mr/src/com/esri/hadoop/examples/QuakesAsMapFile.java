package com.esri.hadoop.examples;
// adapted from Hadoop:_The_Definitive_Guide as well as existing earthquake aggregation sample

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
//import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.GenericOptionsParser;

public class QuakesAsMapFile {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		Configuration config = new Configuration();
		args = new GenericOptionsParser(config, args).getRemainingArgs();
		//System.out.println("Arguments ~ " + args.length + ": " + args[0]);
		String mapFileUri = args[0];
		FileSystem hdfs = FileSystem.get(URI.create(mapFileUri), config);
		Path mapPath = new Path(mapFileUri);
		Path mapData = new Path(mapPath, MapFile.DATA_FILE_NAME);

		SequenceFile.Reader seqRead = new SequenceFile.Reader(hdfs, mapData, config);
		Class keyClass = seqRead.getKeyClass();
		Class valueClass = seqRead.getValueClass();
		seqRead.close();

		long nRec = MapFile.fix(hdfs, mapPath, keyClass, valueClass, false, config);
		System.out.printf("%s contains %d quakes as MapFile.\n", mapPath, nRec);
	}

}
