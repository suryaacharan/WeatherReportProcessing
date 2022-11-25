package com.ucr.mapreduce.weather.reducers;

import com.ucr.mapreduce.weather.MapReduceUtils;

public class Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String text = "690150_55.2*24_2.9";
		String substr = text.substring(text.indexOf("_")+1,text.lastIndexOf("_"));
		System.out.println("1. substr: "+substr);
		String precSubStr = text.substring(text.lastIndexOf("_")+1);
		System.out.println("2. precSubStr "+precSubStr);
		String[] tokens = substr.split("\\*");
		Double value = MapReduceUtils.getDoubleFromString(tokens[0]);
		System.out.println("3. value: "+value);
		Integer ct = MapReduceUtils.getIntegerFromString(tokens[1]);
		System.out.println("4. Ct: "+ct);
		Double precipitation = MapReduceUtils.getDoubleFromString(precSubStr);
		System.out.println("5. precipitation: "+precipitation);
	}

}
