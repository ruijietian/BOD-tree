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
import org.apache.hadoop.io.WritableComparable;

import com.dlmu.BOD_tree.io.TextSerializerHelper;

/**
 *
 * @author 田瑞杰 2021年6月12日
 */
public class Segment implements Shape, WritableComparable<Segment> {
	public double x1;
	public double x2;

	public Segment() {
		this(0, 0);
	}

	public Segment(Segment s) {
		this(s.x1, s.x2);
	}

	/**
	 * @param i
	 * @param j
	 */
	public Segment(double x1, double x2) {
		this.set(x1, x2);
	}

	public void set(double x1, double x2) {
		this.x1 = x1;
		this.x2 = x2;

	}

	public void set(Shape s) {
		if (s == null) {
			System.out.println("tozz");
			return;
		}
		Segment se = s.getSegment();
		set(se.x1, se.x2);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	public void write(DataOutput out) throws IOException {
		out.writeDouble(x1);
		out.writeDouble(x2);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	public void readFields(DataInput in) throws IOException {
		this.x1 = in.readDouble();
		this.x2 = in.readDouble();

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.dlmu.wisdomST.io.TextSerializable#toText(org.apache.hadoop.io.Text)
	 */
	public Text toText(Text text) {
		TextSerializerHelper.serializeDouble(x1, text, ',');
		TextSerializerHelper.serializeDouble(x2, text, '\0');
		return text;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.dlmu.wisdomST.io.TextSerializable#fromText(org.apache.hadoop.io.Text)
	 */
	public void fromText(Text text) {
		x1 = TextSerializerHelper.consumeDouble(text, ',');

		x2 = TextSerializerHelper.consumeDouble(text, '\0');

	}

	public int compareTo(Shape s) {
		Segment seg2 = (Segment) s;
		// Sort by x1 then y1
		if (this.x1 < seg2.x1)
			return -1;
		if (this.x1 > seg2.x1)
			return 1;

		// Sort by x2 then y2
		if (this.x2 < seg2.x2)
			return -1;
		if (this.x2 > seg2.x2)
			return 1;
		return 0;
	}

	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		Segment r2 = (Segment) obj;
		boolean result = this.x1 == r2.x1 && this.x2 == r2.x2;
		return result;
	}

	public Segment getSegment() {
		// TODO Auto-generated method stub
		return new Segment(this);
	}

	public boolean contains(double x) {
		// return x >= x1 && x < x2 && y >= y1 && y < y2;
		return x >= x1 && x <= x2;

	}

	public boolean contains(Segment r) {
		return contains(r.x1, r.x2);
	}

	public boolean contains(double rx1, double rx2) {
		return rx1 >= x1 && rx2 <= x2;
	}

	public Segment union(final Shape s) {
		Segment r = s.getSegment();
		double ux1 = Math.min(x1, r.x1);
		double ux2 = Math.max(x2, r.x2);
		return new Segment(ux1, ux2);
	}

	public void expand(final Shape s) {
		Segment r = s.getSegment();
		if (r.x1 < this.x1)
			this.x1 = r.x1;
		if (r.x2 > this.x2)
			this.x2 = r.x2;
	}

	public IPoint getCenterPoint() {
		return new IPoint((x1 + x2) / 2);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.dlmu.wisdomST.core.Shape#isIntersected(com.dlmu.wisdomST.core.Shape)
	 */

	public Segment getIntersection(Shape s) {
		if (!s.isIntersected(this))
			return null;
		Segment r = s.getSegment();
		double ix1 = Math.max(this.x1, r.x1);
		double ix2 = Math.min(this.x2, r.x2);
		return new Segment(ix1, ix2);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.dlmu.wisdomST.core.Shape#clone()
	 */
	@Override
	public Segment clone() {
		// TODO Auto-generated method stub
		return new Segment(this);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		int result;
		long temp;
		temp = Double.doubleToLongBits(this.x1);
		result = (int) (temp ^ temp >>> 32);
		temp = Double.doubleToLongBits(this.x2);
		result = 31 * result + (int) (temp ^ temp >>> 32);
		return result;
	}

	public double getMaxDistanceTo(double px) {
		double dx = Math.max(px - this.x1, this.x2 - px);

		return dx;
	}

	public double getMinDistanceTo(double px, double py) {
		double dx;
		if (px < this.x1)
			dx = this.x1 - px;
		else if (this.x1 <= px && px <= this.x2)
			dx = 0;
		else
			dx = px - this.x2;
		if (dx > 0)
			return dx;
		return dx;
	}

	@Override
	public String toString() {
		return "Segment: " + x1 + "-" + x2;
	}

	public boolean isValid() {
		return !Double.isNaN(x1);
	}

	public void invalidate() {
		this.x1 = Double.NaN;
	}

	public double getWidth() {
		return x2 - x1;
	}

	public int compareTo(Segment r2) {
		if (this.x1 < r2.x1)
			return -1;
		if (this.x1 > r2.x1)
			return 1;

		if (this.x2 < r2.x2)
			return -1;
		if (this.x2 > r2.x2)
			return 1;

		return 0;
	}

	public Segment buffer(double dw) {
		return new Segment(this.x1 - dw, this.x2 + dw);
	}

	public Segment translate(double dx, double dy) {
		return new Segment(this.x1 + dx, this.x2 + dx);
	}

	public String toWKT() {
		return String.format("SEGMENT((%g %g))", x1, x2);
	}

	public boolean isIntersected(Shape s) {
		Segment r = s.getSegment();
		if (r == null)
			return false;
		// return (this.x2 > r.x1 && r.x2 > this.x1 && this.y2 > r.y1 && r.y2 >
		// this.y1);
		return (this.x2 > r.x1 && r.x2 > this.x1);
	}

}
