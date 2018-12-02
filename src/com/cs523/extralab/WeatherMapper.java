package com.cs523.extralab;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WeatherMapper extends Mapper<LongWritable, Text, Pair, Text> {
  
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	
    	String stationIdValue;
    	String yearValue;
    	String temperatureValue;
    	
    	Text stationId = new Text();
    	Text temperature = new Text();
    	Text year = new Text();
    	
    	Pair pair = new Pair();
    	
    	
        for (String token : value.toString().split("\\s+")) {
        	
        	stationIdValue = token.substring(4,  14);
        	yearValue = token.substring(15, 19);
        	temperatureValue = token.substring(88, 92);
        	
        	stationId.set(stationIdValue);
        	temperature.set(temperatureValue);
        	year.set(yearValue);
        	
        	pair.setStationId(stationId);
        	pair.setTemperature(temperature);
        	
        	context.write(pair, year);
        	
        }
    }
  
}
