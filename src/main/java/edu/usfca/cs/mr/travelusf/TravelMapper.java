package edu.usfca.cs.mr.travelusf;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class TravelMapper
extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        // tokenize into words.
        StringTokenizer itr = new StringTokenizer(value.toString());
        // emit word, count pairs.

        /* Locations Used for itinerary

        9q8zhu -> Golden gate bridge
        87z9py -> Honolulu
        9w2m2v -> Grand canyon
        dk2yqv -> The bahamas
        9q5fh5 -> Griffith Observatory

        We'll use a Heohash accuracy of 5 positions
         */

        /*
        Suitable travel conditions are considered to an ambient temperature
        between 20 to 30 Celsius
        which is 293.15K to 303.15K
         */

        List<String> travel_locations= new ArrayList<>(Arrays.asList("9q8zh","87z9p","9w2m2","dk2yq","9q5fh"));

        int index = 0;
        String timestamp = "";
        String geohash = "";
        Float temp;
        while (itr.hasMoreTokens()) {
            String token = itr.nextToken();
            if(index == 0)
                timestamp = token;
            else if(index == 1)
                geohash = token;
            else if(index == 40)
            {
                temp = Float.parseFloat(token);
                geohash = geohash.substring(0,5);
                if(travel_locations.contains(geohash))
                {
                    if(temp>293 && temp<303)
                    {
                        String record = timestamp+","+temp;
                        context.write(new Text(geohash), new Text(record));
                    }

                }
            }
            index++;
        }
    }
}
