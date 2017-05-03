package cn.jamesxia.graduation.movie_recommend.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Node implements Writable{
	
	private int uId;//不是本来的id，而是映射后的id
	private int mId;//同上
	private float mRate;

	public Node(){
		super();
	}
	public Node(int mId, float mRate) {
		this.mId = mId;
		this.mRate = mRate;
	}
	
	public Node(int uId, int mId, float mRate) {
		this.uId = uId;
		this.mId = mId;
		this.mRate = mRate;
	}
	
	public int getuId() {
		return uId;
	}
	
	public int getmId() {
		return mId;
	}

	public float getRate() {
		return mRate;
	}

	//反序列化
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.mId = in.readInt();
		this.mRate = in.readFloat();
	}

	//序列化
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(mId);
		out.writeFloat(mRate);
	}
}
