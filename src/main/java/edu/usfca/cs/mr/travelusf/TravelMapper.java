package edu.usfca.cs.mr.travelusf;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Mapper: Reads line by line, split them into words. Emit <timestamp, "geohash,temp"> pairs.
 */
public class TravelMapper
extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        // tokenize into words.
        StringTokenizer itr = new StringTokenizer(value.toString());

        /* Locations Used for itinerary

        9q8zhu -> Golden gate bridge
        dr5ru6 -> Empire State Building
        9w2m2v -> Grand canyon
        dk2yqv -> The bahamas
        9q5fh5 -> Griffith Observatory

        We'll use a Geohash accuracy of 4 positions
         */

        /*
        Suitable travel conditions are considered to be an ambient temperature
        between 20 to 30 Celsius
        which is 293.15K to 303.15K
        and no rainfall determined using categorical_rain_yes1_no0_surface
         */

        List<String> travel_locations= new ArrayList<>(Arrays.asList("9q8z","dr5r","9w2m","dk2y","9q5f"));

        int index = 0;
        String timestamp = "";
        String geohash = "";
        Float temp;
        boolean isRainy = true;

        while (itr.hasMoreTokens()) {
            String token = itr.nextToken();
            if(index == 0)
                timestamp = token;
            else if(index == 1)
                geohash = token;
            else if(index == 29)
            {
//                categorical_rain_yes1_no0_surface
                if(Float.parseFloat(token)==0.0)
                {
                    isRainy = false;
                }
            }
            else if(index == 40)
            {
                temp = Float.parseFloat(token);
                geohash = geohash.substring(0,4);
                if(travel_locations.contains(geohash) && !isRainy)
                {
                    if(temp>293 && temp<303)
                    {
                        Date date = new Date(Long.parseLong(timestamp));
                        DateFormat format = new SimpleDateFormat("MM/yyyy");
                        String date_format = format.format(date);
                        String record = geohash+","+temp;
                        context.write(new Text(date_format), new Text(record));
                    }

                }
            }
            index++;
        }
    }
}
