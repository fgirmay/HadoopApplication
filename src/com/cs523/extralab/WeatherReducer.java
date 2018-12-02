package com.cs523.extralab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WeatherReducer extends Reducer<Pair, Text, Pair, Text> {
    private Text result = new Text();
    private Text year = new Text();
    
    @Override
    public void reduce(Pair yearKey, Iterable<Text> yearList, Context context) throws IOException, InterruptedException {
    	
    	int totalTemperature = 0;
    	int tempValue = 0;
    	int counter = 0;
    	int average = 0;
    	
    	for (Text year : yearList) {		
    		context.write(yearKey, year);
    	}	
        
    }
}
