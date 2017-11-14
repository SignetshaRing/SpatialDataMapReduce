package edu.usfca.cs.mr.hottemp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.Array;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * record, list<"timestamp,geohash,temp"> pairs. Checks for the highest temperature among all records.
 * Emits <"record","timestamp,geohash,highest_temp"> record.
 */
public class HotTempReducer
extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(
            Text key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {
        String finalTS = "";
        String finalGeohash = "";
        Float finalTemp = 0f;
        for(Text record : values){
            String rec = record.toString();
            List<String> data = Arrays.asList(rec.split(","));
            String timestamp = data.get(0);
            String geohash = data.get(1);
            Float temp = Float.parseFloat(data.get(2));

            if(temp>finalTemp)
            {
                finalTemp = temp;
                finalTS = timestamp;
                finalGeohash = geohash;
            }
        }

        Date date = new Date(Long.parseLong(finalTS));

        context.write(key, new Text(date+" ,\n"+finalGeohash+", "+finalTemp));
    }

}
