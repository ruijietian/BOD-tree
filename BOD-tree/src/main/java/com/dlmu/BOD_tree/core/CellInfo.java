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
 * Information about a specific cell in a grid. Note: Whenever you change the
 * instance variables that need to be stored in disk, you have to manually fix
 * the implementation of class BlockListAsLongs
 * 
 * @author 田瑞杰
 *
 */
public class CellInfo extends Segment {

	/**
	 * A unique ID for this cell in a file. This must be set initially when cells
	 * for a file are created. It cannot be guessed from cell dimensions.
	 */
	public int cellId;

	/**
	 * 
	 * @param in
	 *            Loads a cell serialized to the given stream
	 * @throws IOException
	 *             流异常
	 * 
	 */

	public CellInfo(DataInput in) throws IOException {
		this.readFields(in);
	}

	public CellInfo(String in) {
		this.fromText(new Text(in));
	}

	public CellInfo() {
		super();
	}

	public CellInfo(int id, double x1, double x2) {
		super(x1, x2);
		this.cellId = id;
	}

	public CellInfo(int id, Segment cellInfo) {
		this(id, cellInfo.x1, cellInfo.x2);
		if (id == 0)
			throw new RuntimeException("Invalid cell id: " + id);
	}

	public CellInfo(CellInfo c) {
		this.set(c);
	}

	public void set(CellInfo c) {
		if (c == null) {
			this.cellId = 0; // Invalid number
		} else {
			super.set(c); // Set rectangle
			this.cellId = c.cellId; // Set cellId
		}
	}

	@Override
	public String toString() {
		return "Cell #" + cellId + " " + super.toString();
	}

	@Override
	public CellInfo clone() {
		return new CellInfo(cellId, x1, x2);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		return ((CellInfo) obj).cellId == this.cellId;
	}

	@Override
	public int hashCode() {
		return (int) this.cellId;
	}

	@Override
	public int compareTo(Shape s) {
		return (int) (this.cellId - ((CellInfo) s).cellId);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(cellId);
		super.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.cellId = in.readInt();
		super.readFields(in);
	}

	@Override
	public Text toText(Text text) {
		TextSerializerHelper.serializeInt(cellId, text, ',');
		return super.toText(text);
	}

	@Override
	public void fromText(Text text) {
		this.cellId = TextSerializerHelper.consumeInt(text, ',');
		super.fromText(text);
	}

	@Override
	public String toWKT() {
		return cellId + "\t" + super.toWKT();
	}

}
