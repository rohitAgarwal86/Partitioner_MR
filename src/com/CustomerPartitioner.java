package com;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomerPartitioner extends Partitioner<IntWritable, PartTransaction>{

	@Override
	public int getPartition(IntWritable Key, PartTransaction value, int noOfReducers) {
		// TODO Auto-generated method stub
		switch(Key.get())
		{
			case 2010:
				return 0;
			case 2011:
				return 1;
			case 2012:
				return 2;
			case 2013:
				return 3;
			default:
				return 0;
				
		}
	}
	

}
