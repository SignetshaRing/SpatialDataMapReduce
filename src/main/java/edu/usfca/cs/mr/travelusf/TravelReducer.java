package edu.usfca.cs.mr.travelusf;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * timestamp(MM/yyyy), list<"geohash,temp"> pairs.
 * Emits <Timestamp, "geohash,temp"> pairs.
 */
public class TravelReducer
extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(
            Text key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {
        for(Text record : values){
            String rec = record.toString();
            List<String> data = Arrays.asList(rec.split(","));
            String geohash = data.get(0);
            Float temp = Float.parseFloat(data.get(1));

            context.write(key,new Text(geohash+", "+Float.toString(temp)));
        }

    }

}
