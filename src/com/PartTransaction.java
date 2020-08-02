package com;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PartTransaction implements Writable{
	
	private String TransactionDate ;
	private int UserId;
	private int Amount ;
	private String ProductType;
	private String Address;
	private String PaymentType ;
	
	public String getTransactionDate() {
		return TransactionDate;
	}
	public void setTransactionDate(String transactionDate) {
		TransactionDate = transactionDate;
	}
	public int getAmount() {
		return Amount;
	}
	public void setAmount(int amount) {
		Amount = amount;
	}
	
	public int getUserId() {
		return UserId;
	}
	public void setUserId(int userId) {
		UserId = userId;
	}
	public String getProductType() {
		return ProductType;
	}
	public void setProductType(String productType) {
		ProductType = productType;
	}
	public String getAddress() {
		return Address;
	}
	public void setAddress(String address) {
		Address = address;
	}
	
	public String getPaymentType() {
		return PaymentType;
	}
	public void setPaymentType(String paymentType) {
		PaymentType = paymentType;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		if((((DataInputStream) in).read())!=-1)
		{
			TransactionDate=in.readUTF();
			UserId=in.readInt();
			Amount=in.readInt();
			ProductType=in.readUTF();
			Address=in.readUTF();
			PaymentType=in.readUTF();
		}
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(TransactionDate);
		out.write(UserId);
		out.writeInt(Amount);
		out.writeUTF(ProductType);
		out.writeUTF(Address);
		out.writeUTF(PaymentType);
		
	}
	
	public void setValues(PartTransaction obj)
	{
		this.TransactionDate=obj.TransactionDate;
		this.UserId=obj.UserId;
		this.Amount=obj.Amount;
		this.PaymentType=obj.PaymentType;
		this.ProductType=obj.ProductType;
		this.Address=obj.Address;
		
	}
}
