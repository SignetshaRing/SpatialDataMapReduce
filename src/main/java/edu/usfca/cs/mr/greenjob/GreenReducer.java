package edu.usfca.cs.mr.greenjob;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * "record", list<"geohash,windspeed,cloudcover,soilporosity"> pairs.
 * Emits <"Wind|Cloud|Combined", "Geohash, Cloudcover, Wind speed"> pairs.
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

        ArrayList<String> cloud_rank = new ArrayList<>();
        ArrayList<String> wind_rank = new ArrayList<>();
        Map<String,Float> combined_rank_map = new TreeMap<>();

        Float soil_porosity;
        int index = 0;
        for(Text record : values) {
            String rec = record.toString();
            List<String> data = Arrays.asList(rec.split(","));
            String geohash = data.get(0);
            Float wind_speed = Float.parseFloat(data.get(1));
            Float cloud_cover = Float.parseFloat(data.get(2));
            soil_porosity = Float.parseFloat(data.get(3));

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
        // Using Rankings system
        // Map has structure <Geohash, Rankings>

        // Creating rank list of wind and cloud maps

        Iterator wind_it = sorted_wind.entrySet().iterator();
        while(wind_it.hasNext())
        {
            Map.Entry pair = (Map.Entry) wind_it.next();
            wind_rank.add((String)pair.getKey());
        }

        // Reversing wind rank, since higher winds are at the end of the list in top_wind_map
        Collections.reverse(wind_rank);

        Iterator cloud_it = sorted_cloud.entrySet().iterator();
        while(cloud_it.hasNext())
        {
            Map.Entry pair = (Map.Entry) cloud_it.next();
            cloud_rank.add((String)pair.getKey());
        }

        for(String geo: wind_rank)
        {
            Float rank = (wind_rank.indexOf(geo)+cloud_rank.indexOf(geo))/(float)2;
            combined_rank_map.put(geo,rank);
        }

        // Sorting Combined rank map
        LinkedHashMap<String,Float> sorted_ranks = OrderByValue(combined_rank_map);


        for(int i = 0;i<top_count;i++)
        {
            String combined_geo = (new ArrayList<String>(sorted_ranks.keySet())).get(i);
            context.write(key,new Text("Combined Wind and Solar farm:" +combined_geo+"\n"+
                    "Cloud: "+top_cloud_map.get(combined_geo)+" Wind: "+top_wind_map.get(combined_geo)));
        }

    }

    /**
     * Fucntion that Sorts an input hashmap by its value
     * @param map
     * @return sorted LinkedHashMap
     */
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
