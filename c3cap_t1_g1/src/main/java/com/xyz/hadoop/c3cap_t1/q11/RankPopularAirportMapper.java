package com.xyz.hadoop.c3cap_t1.q11;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.xyz.hadoop.c3cap_t1.OntimeColumns;

/**
 * TODO Put here a description of what this class does.
 *
 * @author xuyanx.
 *         Created Jan 22, 2018.
 */
public class RankPopularAirportMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text airport = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split(",");
		
		this.airport.set(tokens[OntimeColumns.Origin]);
		context.write(this.airport, one);
		
		this.airport.set(tokens[OntimeColumns.Dest]);
		context.write(this.airport, one);	
	}
}
