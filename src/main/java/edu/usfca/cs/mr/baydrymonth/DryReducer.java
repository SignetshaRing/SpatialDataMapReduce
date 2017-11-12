package edu.usfca.cs.mr.baydrymonth;

import org.apache.hadoop.io.FloatWritable;
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
extends Reducer<Text, Text, Text, FloatWritable> {

    @Override
    protected void reduce(
            Text key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {

        Float total_humid = 0f;
        int index = 0;
        for(Text record : values) {
            String rec = record.toString();

            Float humid = Float.parseFloat(rec);

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
