/****************************************************************************
*
* Copyright© 2021 SEI of the Dalian Maritime University. All Rights Reserved
*
*                   大连海事大学软件工程研究所 版权所有
*
****************************************************************************/
package com.dlmu.BOD_tree.operations;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

import com.dlmu.BOD_tree.core.Segment;
import com.dlmu.BOD_tree.core.Shape;
import com.dlmu.BOD_tree.operations.SpatialRecordReader.ShapeIterator;
import com.dlmu.BOD_tree.util.OperationsParams;

/**
 * Reads a file as a list of RTrees
 * 
 * @author 田瑞杰
 *
 */
public class ShapeIterRecordReader extends SpatialRecordReader<Segment, ShapeIterator> {
	public static final Log LOG = LogFactory.getLog(ShapeIterRecordReader.class);
	private Shape shape;

	public ShapeIterRecordReader(CombineFileSplit split, Configuration conf, Reporter reporter, Integer index)
			throws IOException {
		super(split, conf, reporter, index);
		this.shape = OperationsParams.getShape(conf, "shape");
	}

	public ShapeIterRecordReader(Configuration conf, FileSplit split) throws IOException {
		super(conf, split);
		this.shape = OperationsParams.getShape(conf, "shape");
	}

	public ShapeIterRecordReader(InputStream is, long offset, long endOffset) throws IOException {
		super(is, offset, endOffset);
	}

	public void setShape(Shape shape) {
		this.shape = shape;
	}

	public boolean next(Segment key, ShapeIterator shapeIter) throws IOException {
		// Get cellInfo for the current position in file
		boolean element_read = nextShapeIter(shapeIter);
		key.set(cellMbr); // Set the cellInfo for the last block read
		return element_read;
	}

	public Segment createKey() {
		return new Segment();
	}

	public ShapeIterator createValue() {
		ShapeIterator shapeIter = new ShapeIterator();
		shapeIter.setShape(shape);
		return shapeIter;
	}

}
