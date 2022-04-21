/****************************************************************************
*
* Copyright© 2021 SEI of the Dalian Maritime University. All Rights Reserved
*
*                   大连海事大学软件工程研究所 版权所有
*
****************************************************************************/
package com.dlmu.BOD_tree.indexing;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;

import com.dlmu.BOD_tree.core.ResultCollector;
import com.dlmu.BOD_tree.core.Segment;
import com.dlmu.BOD_tree.core.Shape;

/**
 * A very simple spatial index that provides some spatial operations based on an
 * array storage.
 * 
 * @author 田瑞杰
 *
 * @param <S>
 */
public class GlobalIndex<S extends Shape> implements Writable, Iterable<S> {

	/** A stock instance of S used to deserialize objects from disk */
	protected S stockShape;

	/** All underlying shapes in no specific order */
	protected S[] shapes;

	/** Whether partitions in this global index are compact (minimal) or not */
	private boolean compact;

	/** Whether objects are allowed to replicated in different partitions or not */
	private boolean replicated;

	public GlobalIndex() {
	}

	@SuppressWarnings("unchecked")
	public void bulkLoad(S[] shapes) {
		// Create a shallow copy
		this.shapes = shapes.clone();
		// Change it into a deep copy by cloning each instance
		for (int i = 0; i < this.shapes.length; i++) {
			this.shapes[i] = (S) this.shapes[i].clone();
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(shapes.length);
		for (int i = 0; i < shapes.length; i++) {
			shapes[i].write(out);
		}
	}

	@SuppressWarnings("unchecked")
	public void readFields(DataInput in) throws IOException {
		int length = in.readInt();
		this.shapes = (S[]) new Shape[length];
		for (int i = 0; i < length; i++) {
			this.shapes[i] = (S) stockShape.clone();
			this.shapes[i].readFields(in);
		}
	}

	public int rangeQuery(Shape queryRange, ResultCollector<S> output) {
		int result_count = 0;
		for (S shape : shapes) {
			if (shape.isIntersected(queryRange)) {
				result_count++;
				if (output != null) {
					output.collect(shape);
				}
			}
		}
		return result_count;
	}

	/**
	 * A simple iterator over all shapes in this index
	 * 
	 * @author eldawy
	 *
	 */
	class SimpleIterator implements Iterator<S> {

		/** Current index */
		int i = 0;

		public boolean hasNext() {
			return i < shapes.length;
		}

		public S next() {
			return shapes[i++];
		}

		public void remove() {
			throw new RuntimeException("Not implemented");
		}

	}

	public Iterator<S> iterator() {
		return new SimpleIterator();
	}

	/**
	 * Number of objects stored in the index
	 * 
	 * @return
	 */
	public int size() {
		return shapes.length;
	}

	/**
	 * Returns the minimal bounding rectangle of all objects in the index. If the
	 * index is empty, <code>null</code> is returned.
	 * 
	 * @return - The MBR of all objects or <code>null</code> if empty
	 */
	public Segment getSegment() {
		Iterator<S> i = this.iterator();
		if (!i.hasNext())
			return null;
		Segment globalMBR = new Segment(Double.MAX_VALUE, -Double.MAX_VALUE);
		while (i.hasNext()) {
			globalMBR.expand(i.next().getSegment());
		}
		return globalMBR;
	}

	/**
	 * Returns true if the partitions are compact (minimal) around its contents
	 * 
	 * @return
	 */
	public boolean isCompact() {
		return this.compact;
	}

	public void setCompact(boolean compact) {
		this.compact = compact;
	}

	public void setReplicated(boolean r) {
		this.replicated = r;
	}

	public boolean isReplicated() {
		return replicated;
	}
}
