package edu.usfca.cs.mr.snowdepth;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class SnowDepthMapper
extends Mapper<LongWritable, Text, Text, FloatWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        // tokenize into words.
        StringTokenizer itr = new StringTokenizer(value.toString());
        // emit word, count pairs.
        int index = 0;
        while (itr.hasMoreTokens()) {
            String token = itr.nextToken();
            String geoHash = "";
            if(index == 1)
            {
                geoHash = token;
            }
            else if(index==50)
            {
                FloatWritable record= new FloatWritable(Float.parseFloat(token));
                context.write(new Text(geoHash), record);
            }


            index++;
        }
    }
}
