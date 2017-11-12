package edu.usfca.cs.mr.travelusf;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class TravelReducer
extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(
            Text key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {
        int count = 0;
        // calculate the total count
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
        context.write(key, new Text(finalTS+","+finalGeohash+","+finalTemp.toString()));
    }

}
