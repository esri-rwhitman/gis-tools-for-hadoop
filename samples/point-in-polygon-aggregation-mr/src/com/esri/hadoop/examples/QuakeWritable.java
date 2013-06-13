package com.esri.hadoop.examples;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import com.esri.core.geometry.ogc.OGCGeometry;

public class QuakeWritable implements Writable {

	/*private Text quakeDate;*/
	private GeometryWritable location;
	private DoubleWritable magnitude;

	public QuakeWritable() {
		magnitude = new DoubleWritable();
		location = new GeometryWritable();
	}

	public QuakeWritable(/*String parmDate,*/ OGCGeometry parmLoc, double parmMag) {
		set(/*new Text(parmDate),*/
			new GeometryWritable(parmLoc),
			new DoubleWritable(parmMag));
	}

	//public Text getDate() {return quakeDate;}
	public GeometryWritable getLoc() {return location;}  // beware mutable members
	public DoubleWritable getMag() {return magnitude;}

	@Override
	public void readFields(DataInput inp) throws IOException {
		//quakeDate.readFields(inp);
		location.readFields(inp);
		magnitude.readFields(inp);
	}

	public void set(/*Text parmDate,*/ GeometryWritable parmLoc, DoubleWritable parmMag) {
		//quakeDate = parmDate;
		location = parmLoc;
		magnitude = parmMag;
	}

	/**
	 * This is one record of output of our MapReduce job.
	 */
	@Override
	public String toString() {
		return String.format("%s\t%s", location.toString(), magnitude.toString());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		//quakeDate.write(out);
		location.write(out);
		magnitude.write(out);
	}
}
