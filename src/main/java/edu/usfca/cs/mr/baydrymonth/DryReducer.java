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
        String dry_ts = "";
        Float low_precip = Float.MAX_VALUE;
        String dry_geo = "";
        List<String> bay_area = new ArrayList<>(Arrays.asList("9q8y","9q8v","9q8u","9q8g",
                "9q9n","9q9j","9q9h","9q95","9q97","9q9k","9q9m","9q9q"));
        for(Text record : values){
            String rec = record.toString();
            List<String> data = Arrays.asList(rec.split(","));
            String timestamp = data.get(0);
            String geohash = data.get(1);
            Float precip = Float.parseFloat(data.get(2));

            if(bay_area.contains(geohash))
            {
                if(precip<low_precip)
                {
                    low_precip = precip;
                    dry_ts = timestamp;
                    dry_geo = geohash;
                }
            }
        }

        Date date = new Date(Integer.parseInt(dry_ts));
        DateFormat format = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
//        format.setTimeZone(TimeZone.getTimeZone("Etc/UTC"));
//        String formatted = format.format(date);
//        System.out.println(formatted);
//        format.setTimeZone(TimeZone.getTimeZone("Australia/Sydney"));
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        String date_format = format.format(date);
        int dry_month = cal.get(Calendar.MONTH);

        context.write(key, new Text(dry_geo+", "+low_precip+", "+dry_month+", "+date_format));
    }

}
