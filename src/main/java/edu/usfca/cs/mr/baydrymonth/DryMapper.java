package edu.usfca.cs.mr.baydrymonth;

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
public class DryMapper
extends Mapper<LongWritable, Text, Text, FloatWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        // tokenize into words.
        StringTokenizer itr = new StringTokenizer(value.toString());
        // emit word, count pairs.
        int index = 0;
        String timestamp = "";
        String geohash = "";
        String humid = "";
        while (itr.hasMoreTokens()) {
            String token = itr.nextToken();
            if(index == 0)
                timestamp = token;
            else if(index == 1)
                geohash = token;
            else if(index == 12)
            {
                humid = token;
                // Limiting the data to be MapReduced by only passing
                // geohashes starting with 9q
                List<String> bay_area = new ArrayList<>(Arrays.asList("9q8y","9q8v","9q8u","9q8g",
                        "9q9n","9q9j","9q9h","9q95","9q97","9q9k","9q9m","9q9q"));
                if(bay_area.contains(geohash.substring(0,4)))
                {
                    geohash = geohash.substring(0,4);
                    String record = humid;

                    Date date = new Date(Long.parseLong(timestamp));
                    DateFormat format = new SimpleDateFormat("MM/yyyy");
                    String date_format = format.format(date);


                    context.write(new Text(date_format), new FloatWritable(Float.parseFloat(record)));
                }
            }
            index++;
        }
    }
}
