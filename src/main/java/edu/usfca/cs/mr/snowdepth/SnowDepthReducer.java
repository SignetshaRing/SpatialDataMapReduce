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
 * geohash, list<snowdepth> pairs.  Checks every snowdepth value per Geohash, discards if snoedepth is 0.
 * Emits <geohash, max_snowdepth> pairs.
 */
public class SnowDepthReducer
extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    @Override
    protected void reduce(
            Text key, Iterable<FloatWritable> values, Context context)
    throws IOException, InterruptedException {
        boolean snow_exists = TRUE;

        float max_depth = 0;
        for(FloatWritable val : values){
            if(val.get()>max_depth)
                max_depth = val.get();
            int diff = Float.compare(val.get(),0.0f);
            if(diff==0)
                snow_exists = FALSE;
        }
        if(snow_exists)
            context.write(key, new FloatWritable(max_depth));
    }

}
