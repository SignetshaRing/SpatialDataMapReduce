package edu.usfca.cs.mr.baydrymonth;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class DryMapper
extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        // tokenize into words.
        StringTokenizer itr = new StringTokenizer(value.toString());
        // emit word, count pairs.
        int index = 0;
        String timestamp = "";
        String geohash = "";
        String precip = "";
        while (itr.hasMoreTokens()) {
            String token = itr.nextToken();
            if(index == 0)
                timestamp = token;
            else if(index == 1)
                geohash = token;
            else if(index == 55)
            {
                precip = token;
                // Limiting the data to be MapReduced by only passing
                // geohashes starting with 9q
                if(geohash.substring(0,2).equals("9q"))
                {
                    geohash = geohash.substring(0,4);
                    String record = timestamp+","+geohash+","+precip;
                    context.write(new Text("record"), new Text(record));
                }
            }
            index++;
        }
    }
}
