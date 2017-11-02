package edu.usfca.cs.mr.snowdepth;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;

import java.io.IOException;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class SnowDepthReducer
extends Reducer<Text, IntWritable, Text, FloatWritable> {

    @Override
    protected void reduce(
            Text key, Iterable<IntWritable> values, Context context)
    throws IOException, InterruptedException {
        int count = 0;
        boolean snow_exists = TRUE;
        // calculate the total count
        float max_depth = 0;
        for(IntWritable val : values){
            if(val.get()>max_depth)
                max_depth = val.get();
            if(val.get()==0)
                snow_exists = FALSE;
        }
        if(snow_exists)
            context.write(key, new FloatWritable(max_depth));
    }

}
