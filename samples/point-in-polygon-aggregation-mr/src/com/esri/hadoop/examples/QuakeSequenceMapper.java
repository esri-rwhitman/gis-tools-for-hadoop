package com.esri.hadoop.examples;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.esri.core.geometry.Point;
import com.esri.core.geometry.ogc.OGCPoint;
import com.esri.core.geometry.SpatialReference;


public class QuakeSequenceMapper extends Mapper<LongWritable, Text, Text, QuakeWritable> {
	
	// column indices for values in the CSV
	int longitudeIndex;
	int latitudeIndex;
	SpatialReference spatialReference;
	
	/**
	 * Sets up mapper
	 */
	@Override
	public void setup(Context context)
	{
		Configuration config = context.getConfiguration();

		// first pull values from the configuration
		latitudeIndex = config.getInt("samples.csvdata.columns.lat", 1);
		longitudeIndex = config.getInt("samples.csvdata.columns.long", 2);

		spatialReference = SpatialReference.create(4326);
	}
	
	@Override
	public void map(LongWritable key, Text val, Context context)
			throws IOException, InterruptedException {
		
		/* 
		 * The TextInputFormat we set in the configuration, by default, splits a text file line by line.
		 * The key is the byte offset to the first character in the line.  The value is the text of the line.
		 */
		
		// We know that the first line of the CSV is just headers, so at byte offset 0 we can just return
		if (key.get() == 0) return;

		String line = val.toString();
		String [] values = line.split(",");
		
		// Note: We know the data coming in is clean, but in practice it's best not to
		//       assume clean data.  This is especially true with big data processing
		float latitude = Float.parseFloat(values[latitudeIndex]);
		float longitude = Float.parseFloat(values[longitudeIndex]);
		
		// Create our Point directly from longitude and latitude
		OGCPoint point = new OGCPoint(new Point(longitude, latitude), spatialReference);

		float magnitude = 0;
		try {
			magnitude = Float.parseFloat(values[3]);
		} catch (Exception ignore) {
			// oh well, stay 0
		}
		context.write(new Text(values[0]),
					  new QuakeWritable(point, magnitude));
	}
}
