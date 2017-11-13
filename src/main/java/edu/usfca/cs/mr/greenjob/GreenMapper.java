package edu.usfca.cs.mr.greenjob;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class GreenMapper
extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        // tokenize into words.
        StringTokenizer itr = new StringTokenizer(value.toString());
        // emit word, count pairs.
        int index = 0;

        String geohash = "";
        String cloud_cover = "";
        String wind_speed = "";

        List<String> na_geohash = new ArrayList<>(Arrays.asList("c2","c8","cb","f0","9r","9x","9z","9q",
                "9w","9y","9v","dp","dr",
                "dn","dq","dj"));

        while (itr.hasMoreTokens()) {
            String token = itr.nextToken();
            if(index == 1)
                geohash = token;
            else if(index==15)
                wind_speed = token;
            else if(index == 16)
            {
                cloud_cover = token;

                if(na_geohash.contains(geohash.substring(0,2)))
                {
                    String record = geohash+","+wind_speed+","+cloud_cover;
                    context.write(new Text("record"), new Text(record));
                }
            }
            index++;
        }
    }
}
