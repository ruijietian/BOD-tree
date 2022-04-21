/***********************************************************************
*
* Copyright© 2021 SEI of the Dalian Maritime University. All Rights Reserved
*
*                   大连海事大学软件工程研究所 版权所有
*
*************************************************************************/
package com.dlmu.BOD_tree.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

/**
 *
 * @author 田瑞杰 2021年6月15日 Time-ID(1维)点
 */
public class TIPoint extends IPoint {
	private static final Log LOG = LogFactory.getLog(TIPoint.class);

	public String time;

	public TIPoint() {

	}

	public TIPoint(String text) throws ParseException {

		this.fromText(new Text(text));
	}

	/**
	 * @param x2
	 */
	public void set(double x, String t) {
		this.time = t;
		super.set(x);

	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(time);
		// TODO Auto-generated method stub
		super.write(out);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.time = in.readUTF();
		super.readFields(in);

	}

	@Override
	public Text toText(Text text) {

		byte[] separator = new String(",").getBytes();
		text.append(time.getBytes(), 0, time.getBytes().length);
		text.append(separator, 0, separator.length);
		super.toText(text);
		return text;
	}

	@Override
	public void fromText(Text text) {
		// TODO Auto-generated method stub
		// System.out.println(text.toString());
		String[] list = text.toString().split(",");
		time = list[0];
		super.fromText(new Text(list[1]));
	}

	@Override
	public TIPoint clone() {
		TIPoint c = new TIPoint();
		c.time = this.time;
		c.x = this.x;
		return c;
	}

	@Override
	public String toString() {
		return "TIPoint: (" + x + "-" + time + ")";
	}

}
