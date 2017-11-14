package edu.usfca.cs.mr.baydrymonth;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * date(MM/yyyy), list<humidity> pairs. Calculates the average humidity for each month.
 * Emits <date, average humidity> pairs.
 */
public class DryReducer
extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    @Override
    protected void reduce(
            Text key, Iterable<FloatWritable> values, Context context)
    throws IOException, InterruptedException {

        Float total_humid = 0f;
        int index = 0;
        for(FloatWritable record : values) {
            Float humid = record.get();

            total_humid+=humid;
            index+=1;
        }

        context.write(key, new FloatWritable(total_humid/index));
    }

}
