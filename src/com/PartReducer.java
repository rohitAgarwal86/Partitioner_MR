package com;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PartReducer extends Reducer<IntWritable, PartTransaction, IntWritable, Text>{
	
	//Text outValue =new Text();
	String Output=null;
	Text value=null;
	
	@Override
	public void reduce(IntWritable key,Iterable<PartTransaction> PartObj,Context context) throws IOException, InterruptedException
	{
	//	PartTransaction MaxPart =new PartTransaction();
		//Logger logger = Logger.getLogger(PartReducer.class);
			ArrayList<Integer> UserIds =new ArrayList<Integer>();
			ArrayList<PartTransaction> MaxObj=new ArrayList<PartTransaction>();
			int index=0;
			boolean found=false;
			for (PartTransaction obj:PartObj)
			{
				//logger.debug("inside for");
				//System.err.println("inside for");
				if(UserIds.size()==0)
				{
					//logger.debug("inside if1");
					//System.err.println("inside if");
					UserIds.add(new Integer(obj.getUserId()));
					MaxObj.add(obj);
				}
				else
				{
					//logger.debug("inside else1");
					//System.err.println("inside else1");
					for(Integer UsId:UserIds)
					{
						//logger.debug("inside for2");
						//System.err.println("inside for 2");
						if(UsId==obj.getUserId() && obj !=null)
						{
							//logger.debug("inside if2");
							//System.err.println("inside if2");
							index=UserIds.indexOf(UsId);
							found=true;	
							break;
						}
					}
					if(found)
					{
						//logger.debug("inside found");
						//System.err.println("inside found");
						//MaxPart.setValues(MaxObj.get(index));
						if(obj.getAmount()>MaxObj.get(index).getAmount())
						MaxObj.set(index, obj);
					}
					else
						{
						//logger.debug("inside else2");
						//System.err.println("inside else2");
							UserIds.add(new Integer(obj.getUserId()));
							MaxObj.add(obj);
						}
					}
				}
			for(PartTransaction obj:MaxObj)
			{	
				//index=UserIds.indexOf(UsId);
				//MaxPart.setValues(MaxObj.get(index));
				Output="For the user "+ obj.getUserId() +"Maximum Amount of $"+obj.getAmount()+
						" was spent for "+obj.getProductType()+"at "+obj.getAddress()
						+"on "+obj.getTransactionDate() + "and mode of transaction was " +obj.getPaymentType();
				//logger.debug(Output);
				value=new Text(Output);
				//System.err.println(Output);
				///context.write(key, value);
			}
		}

	}

