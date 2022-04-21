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

import org.apache.hadoop.io.Text;

import com.dlmu.BOD_tree.io.TextSerializerHelper;

/**
 *
 * @author 田瑞杰 2021年6月15日
 */
public class IPoint implements Shape, Comparable<IPoint> {

	public double x;

	public IPoint() {
		this(0);
	}

	public IPoint(double x) {
		set(x);
	}

	/**
	 * A copy constructor from any shape of type Point (or subclass of Point)
	 * 
	 * @param s
	 */
	public IPoint(IPoint s) {
		this.x = s.x;

	}

	public void set(double x) {
		this.x = x;

	}

	public void write(DataOutput out) throws IOException {
		out.writeDouble(x);

	}

	public void readFields(DataInput in) throws IOException {
		this.x = in.readDouble();

	}

	public int compareTo(Shape s) {
		IPoint pt2 = (IPoint) s;

		// Sort by id
		double difference = this.x - pt2.x;
		if (difference == 0)
			return 0;
		return difference > 0 ? 1 : -1;
	}

	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		IPoint r2 = (IPoint) obj;
		return this.x == r2.x;
	}

	@Override
	public int hashCode() {
		int result;
		long temp;
		temp = Double.doubleToLongBits(this.x);
		result = (int) (temp ^ temp >>> 32);

		return result;
	}

	public double distanceTo(IPoint s) {
		double dx = s.x - this.x;

		return dx;
	}

	@Override
	public IPoint clone() {
		return new IPoint(this.x);
	}

	public double distanceTo(double px) {
		double dx = x - px;

		return dx;
	}

	public Shape getIntersection(Shape s) {
		return getSegment().getIntersection(s);
	}

	public boolean isIntersected(Shape s) {
		// TODO Auto-generated method stub
		return getSegment().isIntersected(s);
	}

	@Override
	public String toString() {
		return "Point: (" + x + ")";
	}

	public Text toText(Text text) {
		TextSerializerHelper.serializeDouble(x, text, '\0');

		return text;
	}

	public void fromText(Text text) {
		x = TextSerializerHelper.consumeDouble(text, '\0');

		// System.out.println(x+"-----------"+y);
	}

	public int compareTo(IPoint o) {
		if (x < o.x)
			return -1;
		if (x > o.x)
			return 1;

		return 0;
	}

	public Segment getSegment() {
		// TODO Auto-generated method stub
		return new Segment(x, x);
	}

}
