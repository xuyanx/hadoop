package com.xyz.hadoop.c3cap_t1.q13;

import java.io.IOException;
import java.time.DayOfWeek;
import java.time.format.TextStyle;
import java.util.Locale;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import com.google.common.collect.Lists;

/**
 * TODO Put here a description of what this class does.
 *
 * @author xuyanx.
 *         Created Jan 25, 2018.
 */
public class WeekdayAverageDelayReducer extends Reducer<Text, FloatWritable, Text, Text>{
	@Override
	protected void reduce(Text key, Iterable<FloatWritable> values, Context context)
			throws IOException, InterruptedException {

		//float average = (float) Lists.newArrayList(values).stream().mapToDouble(v -> v.get()).average().getAsDouble();
		
		int count = 0;
		double sum = 0;
		for (FloatWritable v : values) {
			sum += v.get();
			count++;
		}

		float average = (float) (sum / count);
		
		String dayOfWeek = DayOfWeek.of(Integer.parseInt(key.toString())).getDisplayName(TextStyle.FULL_STANDALONE, Locale.US);
		context.write(new Text(dayOfWeek), new Text(String.format("%.2f", average)));
	}
}
