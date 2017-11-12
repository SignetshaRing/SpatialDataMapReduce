package edu.usfca.cs.mr.lightning;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class LightningMapper
extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        // tokenize into words.
        StringTokenizer itr = new StringTokenizer(value.toString());
        // emit word, count pairs.
        int index = 0;
        String geohash = "";
        String light = "";
        while (itr.hasMoreTokens()) {
            String token = itr.nextToken();
            if(index == 1)
                geohash = token.substring(0,5);
            else if(index == 22)
            {
                light = token;
                String record = geohash+","+light;
                context.write(new Text("record"), new Text(record));
            }
            index++;
        }
    }
}
