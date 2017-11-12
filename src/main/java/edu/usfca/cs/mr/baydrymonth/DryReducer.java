package edu.usfca.cs.mr.baydrymonth;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class DryReducer
extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(
            Text key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {
        int count = 0;
        // calculate the total count
        String dry_ts = null;
        Float low_humid = Float.MAX_VALUE;
        String dry_geo = null;
        String date_format = "";
        String dry_month = "";
//        List<String> bay_area = new ArrayList<>(Arrays.asList("9q8y","9q8v","9q8u","9q8g",
//                "9q9n","9q9j","9q9h","9q95","9q97","9q9k","9q9m","9q9q"));
        for(Text record : values) {
            String rec = record.toString();
            List<String> data = Arrays.asList(rec.split(","));
            String timestamp = data.get(0);
            String geohash = data.get(1);
            Float humid = Float.parseFloat(data.get(2));

                if (humid < low_humid) {
                    low_humid = humid;
                    dry_ts = timestamp;
                    dry_geo = timestamp;
                }

        }

        Date date = new Date(Long.parseLong(dry_ts));
        DateFormat format = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        date_format = format.format(date);
        dry_month = Integer.toString(cal.get(Calendar.MONTH));

        context.write(key, new Text(dry_geo+", "+low_humid+", "+dry_month+", "+date_format));
    }

}
