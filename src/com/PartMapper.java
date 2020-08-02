package com;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PartMapper extends Mapper<LongWritable, Text, IntWritable, PartTransaction>{
	
	int DateYear=0;
	
	@Override
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
	{
		PartTransaction part=new PartTransaction();
		IntWritable keyvalue =new IntWritable();
		String tokens[]=value.toString().split(",");
		String dt[] =tokens[0].split("-");
		DateYear =Integer.parseInt(dt[2]);
		
		
		if(tokens.length==6)
		{
			keyvalue.set(DateYear);
			part.setTransactionDate(tokens[0]);
			part.setUserId(Integer.parseInt(tokens[1]));
			part.setAmount(Integer.parseInt(tokens[2]));
			part.setProductType(tokens[3]);
			part.setAddress(tokens[4]);
			part.setPaymentType(tokens[5]);
			context.write(keyvalue, part);
		}
		
		
		
	}

}
