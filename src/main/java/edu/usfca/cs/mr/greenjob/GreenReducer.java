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
        Map<String,Float> top_wind_map = new TreeMap<>();
        Map<String,Float> top_cloud_map = new TreeMap<>();
        Float soil_porosity = 0f;
        int index = 0;
        for(Text record : values) {
            String rec = record.toString();
            List<String> data = Arrays.asList(rec.split(","));
            String geohash = data.get(0);
            Float wind_speed = Float.parseFloat(data.get(1));
            Float cloud_cover = Float.parseFloat(data.get(2));
            soil_porosity = Float.parseFloat(data.get(3));

//            It is observed that for geohashes on land, soil porosity is 0.5 whereas for geohashes on water
//            it is 0.0.
//            context.write(key, new Text("Soil:"+soil_porosity+"Geohash"+geohash));

            if(!wind_map.containsKey(geohash) && soil_porosity.equals(0.5f))
            {
                List wind = new ArrayList();
                wind.add(wind_speed);
                wind_map.put(geohash,wind);
            }
            else if(wind_map.containsKey(geohash) && soil_porosity.equals(0.5f))
            {
                wind_map.get(geohash).add(wind_speed);
            }

            if(!cloud_map.containsKey(geohash) && soil_porosity.equals(0.5f))
            {
                List cloud = new ArrayList();
                cloud.add(cloud_cover);
                cloud_map.put(geohash,cloud);
            }
            else if(cloud_map.containsKey(geohash) && soil_porosity.equals(0.5f))
            {
                cloud_map.get(geohash).add(cloud_cover);
            }

            index+=1;

        }


        Iterator it = wind_map.entrySet().iterator();
        while(it.hasNext())
        {
            Float total_wind = 0f;
            Map.Entry pair = (Map.Entry)it.next();
            for (Float wind:(List<Float>)pair.getValue())
            {
                total_wind+=wind;
            }

            top_wind_map.put((String)pair.getKey(),total_wind/((List<Float>)pair.getValue()).size());

        }

        Iterator it2 = cloud_map.entrySet().iterator();
        while(it2.hasNext())
        {
            Float total_cloud = 0f;
            Map.Entry pair = (Map.Entry)it2.next();
            for (Float cloud:(List<Float>)pair.getValue())
            {
                total_cloud+=cloud;
            }

            top_cloud_map.put((String)pair.getKey(),total_cloud/((List<Float>)pair.getValue()).size());

        }

        // Sorting the top cloud and wind maps(Increasing order)

        LinkedHashMap<String,Float> sorted_cloud = OrderByValue(top_cloud_map);
        LinkedHashMap<String,Float> sorted_wind = OrderByValue(top_wind_map);

        // Output top3 values
        int top_count = 3;

        for(int i = 0;i<top_count;i++)
        {
            String cloud_key = (new ArrayList<String>(sorted_cloud.keySet())).get(i);
            Float value = (new ArrayList<Float>(sorted_cloud.values())).get(i);
            context.write(key,new Text("Cloud: "+cloud_key+" "+Float.toString(value)));
        }

        for(int i = 0;i<top_count;i++)
        {
            String wind_key = (new ArrayList<String>(sorted_wind.keySet())).get(sorted_wind.size()-i-1);
            Float value = (new ArrayList<Float>(sorted_wind.values())).get(sorted_wind.size()-i-1);
            context.write(key,new Text("Wind: "+wind_key+" "+Float.toString(value)));
        }


        // Location for combined wind and solar farm
        Float sum = 0f;
        for(int i = sorted_wind.size()/2;i<sorted_wind.size();i++)
        {
            sum+=(new ArrayList<Float>(sorted_wind.values())).get(i);
        }
        Float top_avg_wind = sum/(sorted_wind.size()/2);

        sum = 0f;
        for(int i = 0;i<sorted_wind.size()/2;i++)
        {
            sum+=(new ArrayList<Float>(sorted_cloud.values())).get(i);
        }
        Float low_avg_cloud = sum/(sorted_cloud.size()/2);

        for(Text record : values) {
            String rec = record.toString();
            List<String> data = Arrays.asList(rec.split(","));
            String geohash = data.get(0);
            Float wind_speed = Float.parseFloat(data.get(1));
            Float cloud_cover = Float.parseFloat(data.get(2));
            soil_porosity = Float.parseFloat(data.get(3));
        }

    }


    public LinkedHashMap<String,Float> OrderByValue(Map<String,Float> map)
    {
        Set<Map.Entry<String, Float>> set = map.entrySet();
        List<Map.Entry<String, Float>> list = new ArrayList<Map.Entry<String, Float>>(set);
        Collections.sort( list, new Comparator<Map.Entry<String, Float>>()
        {
            public int compare( Map.Entry<String, Float> o1, Map.Entry<String, Float> o2 )
            {
                return o1.getValue().compareTo(o2.getValue());
            }
        } );


        LinkedHashMap<String, Float> sortedMap = new LinkedHashMap<String, Float>();
        for (Map.Entry<String, Float> entry : list) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }

}
