/****************************************************************************
*
* Copyright© 2021 SEI of the Dalian Maritime University. All Rights Reserved
*
*                   大连海事大学软件工程研究所 版权所有
*
****************************************************************************/
package com.dlmu.BOD_tree.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import com.dlmu.BOD_tree.io.TextSerializerHelper;

/**
 * @author 田瑞杰
 */

public class Partition extends CellInfo {
	/** Name of the file that contains the data */
	public String filename;

	/** Total number of records in this partition */
	public long recordCount;

	/** Total size of data in this partition in bytes (uncompressed) */
	public long size;

	public Partition() {
	}

	public Partition(String filename, CellInfo cell) {
		this.filename = filename;
		super.set(cell);
	}

	public Partition(Partition other) {
		this.filename = other.filename;
		this.recordCount = other.recordCount;
		this.size = other.size;
		super.set((CellInfo) other);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeUTF(filename);
		out.writeLong(recordCount);
		out.writeLong(size);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		filename = in.readUTF();
		this.recordCount = in.readLong();
		this.size = in.readLong();
	}

	@Override
	public Text toText(Text text) {
		super.toText(text);
		text.append(new byte[] { ',' }, 0, 1);
		TextSerializerHelper.serializeLong(recordCount, text, ',');
		TextSerializerHelper.serializeLong(size, text, ',');
		byte[] temp = (filename == null ? "" : filename).getBytes();
		text.append(temp, 0, temp.length);
		return text;
	}

	@Override
	public void fromText(Text text) {
		super.fromText(text);
		text.set(text.getBytes(), 1, text.getLength() - 1); // Skip comma
		this.recordCount = TextSerializerHelper.consumeLong(text, ',');
		this.size = TextSerializerHelper.consumeLong(text, ',');
		filename = text.toString();
	}

	@Override
	public Partition clone() {
		return new Partition(this);
	}

	@Override
	public int hashCode() {
		return filename.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		return this.filename.equals(((Partition) obj).filename);
	}

	public void expand(Partition p) {
		super.expand(p);
		// accumulate size
		this.size += p.size;
		this.recordCount += p.recordCount;
	}

	@Override
	public String toWKT() {
		return super.toWKT() + "\t" + recordCount + "\t" + size + "\t" + filename;
	}

}
