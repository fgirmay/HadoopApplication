package com.cs523.extralab;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class WeatherReducer extends Reducer<Pair, Text, Pair, Text> {
    
    @Override
    public void reduce(Pair yearKey, Iterable<Text> yearList, Context context) throws IOException, InterruptedException {
    	
    	for (Text year : yearList) {		
    		context.write(yearKey, year);
    	}	
        
    }
}
