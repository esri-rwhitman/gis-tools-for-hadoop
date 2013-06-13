package com.esri.hadoop.examples;
// adapted from Hadoop:_The_Definitive_Guide as well as existing earthquake aggregation sample

//import java.io.ByteArrayOutputStream;
import java.io.File;
//import java.io.FileOutputStream;
import java.net.URI;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
//import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
//import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
//import org.apache.avro.io.Decoder;
//import org.apache.avro.io.Encoder;
//import org.apache.avro.io.DecoderFactory;
//import org.apache.avro.io.EncoderFactory;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;

//import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCPoint;

public class QuakesAsAvro {

	public static void main(String[] args) throws Exception {
		Configuration config = new Configuration();
		args = new GenericOptionsParser(config, args).getRemainingArgs();
		//System.out.println("Arguments ~ " + args.length + ": " + args[0]);
		String seqFileUri = args[1];
		FileSystem hdfs = FileSystem.get(URI.create(seqFileUri), config);
		Path path = new Path(seqFileUri);
		SequenceFile.Reader seqRead = new SequenceFile.Reader(hdfs, path, config);

		//String schemaFile = args[0];  // Earthquake.avsc
		String schemaStr = "{\"type\":\"record\",\"name\":\"Earthquake\",\"fields\":[{\"name\":\"qdate\",\"type\":\"string\"},{\"name\":\"latitude\",\"type\":\"double\"},{\"name\":\"longitude\",\"type\":\"double\"},{\"name\":\"magnitude\",\"type\":\"double\"}]}";
		//Schema schema = Schema.parse(schemaStr);
		Schema.Parser avroParser = new Schema.Parser();
		Schema schema = avroParser.parse(schemaStr);
		GenericRecord datum = new GenericData.Record(schema);
		//ByteArrayOutputStream out = new ByteArrayOutputStream();
		//FileOutputStream fou = new FileOutputStream("test.avro");
		File file = new File("test.avro");
		DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
		//Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(writer);

		try {
			dataFileWriter.create(schema, file);
			Text key = (Text)ReflectionUtils.newInstance(seqRead.getKeyClass(), config);
			QuakeWritable value = (QuakeWritable)ReflectionUtils.newInstance(seqRead.getValueClass(), config);
			while (seqRead.next(key, value)) {
				OGCPoint point = (OGCPoint)(value.getLoc().getGeom());
				datum.put("qdate", new Utf8(key.toString()));
				datum.put("latitude", point.Y());
				datum.put("longitude", point.X());
				datum.put("magnitude", value.getMag().get());
				//writer.write(datum, encoder);
				dataFileWriter.append(datum);
			}
		} finally {
			//encoder.flush();
			//out.close();
			dataFileWriter.close();
			if (seqRead != null)
				seqRead.close();
		}

		// This output looked OK.
		// DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
		// Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
		// GenericRecord result;
		// result = reader.read(null, decoder);
		// System.out.println(result.toString());
		// result = reader.read(null, decoder);
		// System.out.println(result.toString());
		//fou.write(out.toByteArray());
		//fou.flush();
		//fou.close();
	}

}
