package com.xyz.hadoop.c3cap_t1.q13;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.xyz.hadoop.c3cap_t1.OntimeColumns;

/**
 * TODO Put here a description of what this class does.
 *
 * @author xuyanx.
 *         Created Jan 25, 2018.
 */
public class WeekdayAverageDelayMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
	private final FloatWritable delay = new FloatWritable(0);
	private Text weekday = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split(",", -1);

		if (!tokens[OntimeColumns.ArrDelay].isEmpty()) {
			this.weekday.set(tokens[OntimeColumns.DayOfWeek]);	
			this.delay.set(new Float(tokens[OntimeColumns.ArrDelay]));
			context.write(this.weekday, this.delay);
		}
	}
}
