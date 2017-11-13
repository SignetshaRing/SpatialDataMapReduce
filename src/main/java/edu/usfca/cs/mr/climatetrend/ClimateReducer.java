package edu.usfca.cs.mr.climatetrend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class ClimateReducer
extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(
            Text key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {
        List<Float> tempList = new ArrayList<>();
        List<Float> precipList = new ArrayList<>();
        String geohash ="";

        for(Text record : values){
            String rec = record.toString();
            List<String> data = Arrays.asList(rec.split(","));
            geohash = data.get(0);
            Float temp = Float.parseFloat(data.get(1));
            Float precip = Float.parseFloat(data.get(2));

            tempList.add(temp);
            precipList.add(precip);
        }

        Collections.sort(tempList);
        Collections.sort(precipList);

        Float minTemp = tempList.get(0);
        Float maxTemp = tempList.get(tempList.size()-1);

        Float minPrecip = precipList.get(0);
        Float maxPrecip = precipList.get(precipList.size()-1);

        Float tsum = 0f;
        for(Float t : tempList)tsum+=t;
        Float avgTemp = tsum/tempList.size();

        Float psum = 0f;
        for(Float p : precipList)psum+=p;
        Float avgPrecip = psum/precipList.size();

        //Output to File Format
        // <month-num>  <high-temp>  <low-temp>  <avg-precip>  <avg-temp>

        int month_num = Integer.parseInt(key.toString().substring(0,2));

        String output = month_num+" "+maxTemp+" "+minTemp+" "+avgPrecip+" "+avgTemp;

        context.write(key, new Text(output));

        String file_name = "Climate_Trend_"+geohash+".txt";
        File f = new File(file_name);

        if(f.exists())
        {
            try(FileWriter fw = new FileWriter(file_name, true);
                BufferedWriter bw = new BufferedWriter(fw);
                PrintWriter out = new PrintWriter(bw))
            {
                out.println(output);
            } catch (IOException e) {
                System.out.println("Error while writing to File");
            }
        }
        else
        {
            try(FileWriter fw = new FileWriter(file_name);
                BufferedWriter bw = new BufferedWriter(fw);
                PrintWriter out = new PrintWriter(bw))
            {
                out.println(output);
            } catch (IOException e) {
                System.out.println("Error while writing to File");
            }
        }


    }

}
