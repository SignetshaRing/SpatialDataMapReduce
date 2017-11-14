package edu.usfca.cs.mr.hottemp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Mapper: Reads line by line, split them into words. Emit <"record", "timestamp,geohash,temp"> pairs.
 */
public class HotTempMapper
extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        // tokenize into words.
        StringTokenizer itr = new StringTokenizer(value.toString());
        int index = 0;
        String timestamp = "";
        String geohash = "";
        String temp;
        while (itr.hasMoreTokens()) {
            String token = itr.nextToken();
            if(index == 0)
                timestamp = token;
            else if(index == 1)
                geohash = token;
            else if(index == 40)
            {
                temp = token;
                String record = timestamp+","+geohash+","+temp;
                context.write(new Text("record"), new Text(record));
            }
            index++;
        }
    }
}
