package com.esri.hadoop.examples;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.esri.core.geometry.ogc.OGCGeometry;

public class GeometryWritable implements Writable {

	private OGCGeometry geometry;

	public GeometryWritable() {}

	public GeometryWritable(OGCGeometry parmGeom) {
		set(parmGeom);
	}

	public OGCGeometry getGeom() {return geometry;}

	@Override
	public void readFields(DataInput inp) throws IOException {
		int gLen = WritableUtils.readVInt(inp);
		byte[] byteArr = new byte[gLen];
		inp.readFully(byteArr, 0, gLen);
		ByteBuffer byteBuf = ByteBuffer.wrap(byteArr);
		geometry = OGCGeometry.fromBinary(byteBuf);
	}

	public void set(OGCGeometry parmGeom) {
		geometry = parmGeom;
	}

	/**
	 * This is one record of output of our MapReduce job.
	 */
	@Override
	public String toString() {
		return getGeom().asText();
	}

	@Override
	public void write(DataOutput out) throws IOException {
			ByteBuffer byteBuf = geometry.asBinary();
			byte[] byteArr = byteBuf.array();
			WritableUtils.writeVInt(out, byteArr.length);
			out.write(byteArr, 0, byteArr.length);
	}

}
