/****************************************************************************
*
* Copyright© 2021 SEI of the Dalian Maritime University. All Rights Reserved
*
*                   大连海事大学软件工程研究所 版权所有
*
****************************************************************************/
package com.dlmu.BOD_tree.indexing;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.mortbay.log.Log;

import com.dlmu.BOD_tree.core.CellInfo;
import com.dlmu.BOD_tree.core.IPoint;
import com.dlmu.BOD_tree.core.ResultCollector;
import com.dlmu.BOD_tree.core.Segment;
import com.dlmu.BOD_tree.core.Shape;

/**
 * An interface for spatially partitioning data into partitions.
 * 
 * @author 田瑞杰
 *
 */
public abstract class Partitioner implements Writable {
	/** Configuration line for partitioner class */
	private static final String PartitionerClass = "Partitioner.Class";
	private static final String PartitionerValue = "Partitioner.Value";

	/**
	 * Populate this partitioner for a set of points and number of partitions
	 * 
	 * @param mbr
	 *            - the minimal bounding rectangle of the input space
	 * @param points
	 *            - the points to be partitioned
	 * @param capacity
	 *            - maximum number of points per partition
	 * @throws IllegalArgumentException
	 *             if points are empty
	 */
	public abstract void createFromPoints(Segment mbr, IPoint[] points, int capacity) throws IllegalArgumentException;

	/**
	 * Overlap a shape with partitions and calls a matcher for each overlapping
	 * partition.
	 * 
	 * @param shape
	 * @param matcher
	 */

	public abstract void overlapPartitions(Shape shape, ResultCollector<Integer> matcher);

	/**
	 * Returns only one overlapping partition. If the given shape overlaps more than
	 * one partitions, the partitioner returns only one of them according to its own
	 * criteria. If it does not overlap any partition, it returns -1 which is an
	 * invalid partition ID.
	 * 
	 * @param shape
	 * @return
	 */

	public abstract int overlapPartition(Shape shape);

	/**
	 * Returns the details of a specific partition given its ID.
	 * 
	 * @param partitionID
	 * @return
	 */

	public abstract CellInfo getPartition(int partitionID);

	/**
	 * Returns the detail of a partition given its index starting at zero and ending
	 * at partitionCount() - 1
	 * 
	 * @param index
	 * @return
	 */

	public abstract CellInfo getPartitionAt(int index);

	/**
	 * Returns total number of partitions
	 * 
	 * @return
	 */
	public abstract int getPartitionCount();

	/**
	 * Sets the class and value of a partitioner in the given job
	 * 
	 * @param conf
	 * @param partitioner
	 * @throws IOException
	 */
	public static void setPartitioner(Configuration conf, Partitioner partitioner) throws IOException {
		conf.setClass(PartitionerClass, partitioner.getClass(), Partitioner.class);
		Path tempFile;
		FileSystem fs = FileSystem.get(conf);
		do {
			tempFile = new Path("cells_" + (int) (Math.random() * 1000000) + ".partitions");
		} while (fs.exists(tempFile));
		FSDataOutputStream out = fs.create(tempFile);
		partitioner.write(out);
		out.close();

		fs.deleteOnExit(tempFile);

		DistributedCache.addCacheFile(tempFile.toUri(), conf);
		conf.set(PartitionerValue, tempFile.getName());
	}

	/**
	 * Retrieves the value of a partitioner for a given job.
	 * 
	 * @param conf
	 * @return
	 */
	public static Partitioner getPartitioner(Configuration conf) {
		Class<? extends Partitioner> klass = conf.getClass(PartitionerClass, Partitioner.class)
				.asSubclass(Partitioner.class);
		if (klass == null)
			return null;
		try {
			Partitioner partitioner = klass.newInstance();

			String partitionerFile = conf.get(PartitionerValue);
			if (partitionerFile != null) {
				Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
				for (Path cacheFile : cacheFiles) {
					if (cacheFile.getName().contains(partitionerFile)) {
						FSDataInputStream in = FileSystem.getLocal(conf).open(cacheFile);
						partitioner.readFields(in);
						in.close();
					}
				}
			}
			return partitioner;
		} catch (InstantiationException e) {
			Log.warn("Error instantiating partitioner", e);
			return null;
		} catch (IllegalAccessException e) {
			Log.warn("Error instantiating partitioner", e);
			return null;
		} catch (IOException e) {
			Log.warn("Error retrieving partitioner value", e);
			return null;
		}
	}

}
