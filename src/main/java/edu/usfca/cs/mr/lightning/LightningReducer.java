package edu.usfca.cs.mr.lightning;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class LightningReducer
extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(
            Text key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {

        String topGeohash = "";
        String top2Geohash = "";
        String top3Geohash = "";
        Float topLightning = 0f;
        Float top2Lightning = 0f;
        Float top3Lightning = 0f;
        for(Text record : values){
            String rec = record.toString();
            List<String> data = Arrays.asList(rec.split(","));
            String geohash = data.get(0);
            Float lightning = Float.parseFloat(data.get(1));

            if(lightning>topLightning)
            {
                topLightning = lightning;
                topGeohash = geohash;
            }
            else if(lightning>top2Lightning)
            {
                top2Lightning = lightning;
                top2Geohash = geohash;
            }
            else if(lightning>top3Lightning)
            {
                top3Lightning = lightning;
                top3Geohash = geohash;
            }
        }

        // Since only top 3 geohashes were required separate variables for each were used
        // as opposed to storing all geohashes and sorting according to Lightning values
        // as that would occur a significant time penalty O(nLog(n))
        context.write(key, new Text(topGeohash+","+topLightning.toString()));
        context.write(key, new Text(top2Geohash+","+top2Lightning.toString()));
        context.write(key, new Text(top3Geohash+","+top3Lightning.toString()));
    }

}
