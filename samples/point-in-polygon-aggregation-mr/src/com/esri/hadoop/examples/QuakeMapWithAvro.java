package com.esri.hadoop.examples;

import java.io.IOException;
import java.io.FileWriter;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.avro.mapred.*;
import org.apache.avro.mapreduce.*;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.QuadTree;
import com.esri.core.geometry.QuadTree.QuadTreeIterator;
import com.esri.core.geometry.SpatialReference;
import com.esri.json.EsriFeatureClass;


public class QuakeMapWithAvro extends Mapper<AvroKey<Earthquake>, NullWritable, Text, IntWritable> {
	
	// column indices for values in the CSV
	int longitudeIndex;
	int latitudeIndex;
	

	// in ca_counties.json, the label for the polygon is "NAME"
	String labelAttribute;
	
	EsriFeatureClass featureClass;
	SpatialReference spatialReference;
	QuadTree quadTree;
	QuadTreeIterator quadTreeIter;
	
	private void buildQuadTree(){
		quadTree = new QuadTree(new Envelope2D(-180, -90, 180, 90), 8);
		
		Envelope envelope = new Envelope();
		for (int i=0;i<featureClass.features.length;i++){
			featureClass.features[i].geometry.queryEnvelope(envelope);
			quadTree.insert(i, new Envelope2D(envelope.getXMin(), envelope.getYMin(), envelope.getXMax(), envelope.getYMax()));
		}
		
		quadTreeIter = quadTree.getIterator();
	}
	
	/**
	 * Query the quadtree for the feature containing the given point
	 * 
	 * @param pt point as longitude, latitude
	 * @return index to feature in featureClass or -1 if not found
	 */
	private int queryQuadTree(Point pt)
	{
		// reset iterator to the quadrant envelope that contains the point passed
		quadTreeIter.resetIterator(pt, 0);
		
		int elmHandle = quadTreeIter.next();
		
		while (elmHandle >= 0){
			int featureIndex = quadTree.getElement(elmHandle);
			
			// we know the point and this feature are in the same quadrant, but we need to make sure the feature
			// actually contains the point
			if (GeometryEngine.contains(featureClass.features[featureIndex].geometry, pt, spatialReference)){
				return featureIndex;
			}
			
			elmHandle = quadTreeIter.next();
		}
		
		// feature not found
		return -1;
	}
	
	
	/**
	 * Sets up mapper with filter geometry provided as argument[0] to the jar
	 */
	@Override
	public void setup(Context context)
	{
		Configuration config = context.getConfiguration();

		// first pull values from the configuration		
		String featuresPath = config.get("sample.features.input");
		labelAttribute = config.get("sample.features.keyattribute", "NAME");
		
		FSDataInputStream iStream = null;
		
		spatialReference = SpatialReference.create(4326);
		
		try {
			// load the JSON file provided as argument 0
			FileSystem hdfs = FileSystem.get(config);
			iStream = hdfs.open(new Path(featuresPath));
			featureClass = EsriFeatureClass.fromJson(iStream);
		} 
		catch (Exception e)
		{
			e.printStackTrace();
		} 
		finally
		{
			if (iStream != null)
			{
				try {
					iStream.close();
				} catch (IOException e) { }
			}
		}
		
		// build a quadtree of our features for fast queries
		if (featureClass != null){
			buildQuadTree();
		}
	}
	
	@Override
	public void map(AvroKey<Earthquake> key, NullWritable val, Context context)
			throws IOException, InterruptedException {

		Earthquake quake = key.datum();
		Point point = new Point(quake.longitude, quake.latitude);
		
		// Each map only processes one earthquake record at a time, so we start out with our count 
		// as 1.  Aggregation will occur in the combine/reduce stages.
		IntWritable one = new IntWritable(1);
		
		int featureIndex = queryQuadTree(point);
		
		if (featureIndex >= 0){
			String name = (String)featureClass.features[featureIndex].attributes.get(labelAttribute);
			
			if (name == null) 
				name = "???";
			
			context.write(new Text(name), one);
		} else {
			context.write(new Text("*Outside Feature Set"), one);
		}
	}
}
