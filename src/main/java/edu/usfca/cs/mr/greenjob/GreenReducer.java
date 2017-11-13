package edu.usfca.cs.mr.greenjob;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

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

        Map<String,List<Float>> wind_map = new TreeMap<>();
        Map<String,List<Float>> cloud_map = new TreeMap<>();
        int index = 0;
        for(Text record : values) {
            String rec = record.toString();
            List<String> data = Arrays.asList(rec.split(","));
            String geohash = data.get(0);
            Float wind_speed = Float.parseFloat(data.get(1));
            Float cloud_cover = Float.parseFloat(data.get(2));

            if(!wind_map.containsKey(geohash))
            {
                List wind = new ArrayList();
                wind.add(wind_speed);
                wind_map.put(geohash,wind);
            }
            else
            {
                wind_map.get(geohash).add(wind_speed);
            }

            if(!cloud_map.containsKey(geohash))
            {
                List cloud = new ArrayList();
                cloud.add(cloud_cover);
                wind_map.put(geohash,cloud);
            }
            else
            {
                cloud_map.get(geohash).add(cloud_cover);
            }

            index+=1;

        }


        Iterator it = wind_map.entrySet().iterator();
        while(it.hasNext())
        {
//            Float total_wind = 0;
            Map.Entry pair = (Map.Entry)it.next();
            for (Float wind:(List<Float>)pair.getValue()) {

            }
        }

//        Date date = new Date(Long.parseLong(dry_ts));
//        DateFormat format = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
//        Calendar cal = Calendar.getInstance();
//        cal.setTime(date);
//        date_format = format.format(date);
//        dry_month = Integer.toString(cal.get(Calendar.MONTH));

//        context.write(key, new FloatWritable(total_humid/index));
    }

}
