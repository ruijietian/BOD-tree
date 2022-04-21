/***********************************************************************
*
* Copyright© 2021 SEI of the Dalian Maritime University. All Rights Reserved
*
*                   大连海事大学软件工程研究所 版权所有
*
*************************************************************************/
package com.dlmu.BOD_tree.core;

import org.apache.hadoop.io.Writable;

import com.dlmu.BOD_tree.io.TextSerializable;

/**
 * A general 1D shape.
 * 
 * @author 田瑞杰
 *
 */
public interface Shape extends Writable, Cloneable, TextSerializable {
	/**
	 * Returns minimum bounding rectangle for this shape.
	 * 
	 * @return The minimum bounding rectangle for this shape
	 */
	public Segment getSegment();

	/**
	 * Returns true if this shape is intersected with the given shape
	 * 
	 * @param s
	 *            The other shape to test for intersection with this shape
	 * @return <code>true</code> if this shape intersects with s; <code>false</code>
	 *         otherwise.
	 */
	public boolean isIntersected(final Shape s);

	/**
	 * Returns a clone of this shape
	 * 
	 * @return A new object which is a copy of this shape
	 */
	public Shape clone();

}
