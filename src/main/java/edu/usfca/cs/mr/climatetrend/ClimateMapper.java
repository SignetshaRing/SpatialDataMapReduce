package edu.usfca.cs.mr.climatetrend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class ClimateMapper
extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        // tokenize into words.
        StringTokenizer itr = new StringTokenizer(value.toString());
        // emit word, count pairs.
        int index = 0;
        String timestamp = "";
//        String target_geohash = "9q8ytxu5et";
//        String target_geohash = "d59d5yttuc5b";
        String target_geohash = "d5dpds10m55b";
        String geohash = "";
        String temp = "";
        String precip = "";
        while (itr.hasMoreTokens()) {
            String token = itr.nextToken();
            if(index == 0)
                timestamp = token;
            else if(index == 1)
                geohash = token;
            else if(index == 40)
            {
                temp = token;
//                String record = timestamp+","+geohash+","+temp;
//                context.write(new Text("record"), new Text(record));
            }
            else if(index == 55)
            {
                precip = token;
                if(target_geohash.equals(geohash))
                {
                    Date date = new Date(Long.parseLong(timestamp));
                    DateFormat format = new SimpleDateFormat("MM/yyyy");
                    String date_format = format.format(date);

                    String record = geohash+","+temp+","+precip;
                    context.write(new Text(date_format), new Text(record));
                }
            }
            index++;
        }
    }
}
