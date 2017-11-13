package edu.usfca.cs.mr.greenjob;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class GreenReducer
extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(
            Text key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {

        Map<String,Float> wind_map = new TreeMap<>();
        Map<String,Float> cloud_map = new TreeMap<>();
        int index = 0;
        for(Text record : values) {
            String rec = record.toString();
            List<String> data = Arrays.asList(rec.split(","));
            String geohash = data.get(0);
            Float wind_speed = Float.parseFloat(data.get(1));
            Float cloud_cover = Float.parseFloat(data.get(2));

            total_humid+=humid;
            index+=1;

        }

//        Date date = new Date(Long.parseLong(dry_ts));
//        DateFormat format = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
//        Calendar cal = Calendar.getInstance();
//        cal.setTime(date);
//        date_format = format.format(date);
//        dry_month = Integer.toString(cal.get(Calendar.MONTH));

        context.write(key, new FloatWritable(total_humid/index));
    }

}
