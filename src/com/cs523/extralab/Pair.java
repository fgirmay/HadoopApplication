package com.cs523.extralab;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Pair implements WritableComparable<Pair> {
	
	private Text stationId = new Text();
	private Text temperature = new Text();
	
	@Override
	public void readFields(DataInput in) throws IOException {
		stationId.readFields(in);
		temperature.readFields(in);
	}


	@Override
	public void write(DataOutput out) throws IOException {
		
		stationId.write(out);
		temperature.write(out);
	}


	@Override
	public int compareTo(Pair pair) {
		
		int cmp = this.stationId.compareTo(pair.stationId);
		 
        if (cmp != 0) {
            return cmp;
        }
 
        return pair.temperature.compareTo(this.temperature);
	}
	
	@Override
	public int hashCode() {
		return stationId.hashCode()*163 + temperature.hashCode();
	}
	
	public Text getStationId() {
		return this.stationId;
	}
	
	public void setStationId(Text stationId) {
		this.stationId = stationId;
	}
	
	public Text getTemperature() {
		return this.temperature;
	}
	
	public void setTemperature(Text temperature) {
		this.temperature = temperature;
	}
	
	@Override
    public boolean equals(Object o) {
		
        if(o instanceof Pair) {
        	
            Pair pair = (Pair) o;
            
            return stationId.equals(pair.stationId) && temperature.equals(pair.temperature);
        }
        return false;
    }
	
	@Override
    public String toString() {
		
        return stationId + " " + temperature;
    }
	

}
