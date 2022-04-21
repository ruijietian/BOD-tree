/***********************************************************************
*
* Copyright© 2021 SEI of the Dalian Maritime University. All Rights Reserved
*
*                   大连海事大学软件工程研究所 版权所有
*
*************************************************************************/
package com.dlmu.BOD_tree.indexing;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Vector;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.util.GenericOptionsParser;

import com.dlmu.BOD_tree.core.CellInfo;
import com.dlmu.BOD_tree.core.IPoint;
import com.dlmu.BOD_tree.core.ResultCollector;
import com.dlmu.BOD_tree.core.Segment;
import com.dlmu.BOD_tree.core.Shape;
import com.dlmu.BOD_tree.operations.ShapeIterRecordReader;
import com.dlmu.BOD_tree.operations.SpatialRecordReader.ShapeIterator;
import com.dlmu.BOD_tree.util.OperationsParams;

/**
 *
 * @author 田瑞杰 2021年6月16日
 */
public class BOD_tree extends Partitioner {
	/** MBR of the input file */
	private final Segment seg = new Segment();

	/**
	 * Location of all splits stored in a complete binary tree which is encoded in a
	 * single array in a heap-like structure.
	 */
	private double[] splits;
	/**
	 * 用map集合存储分区以及应该切分出去的异常数据数量
	 */
	private Map<Integer, Integer> lmap = new HashMap<Integer, Integer>();

	/**
	 * A default constructor to be able to dynamically instantiate it and
	 * deserialize it
	 */
	public BOD_tree() {
	}

	public void write(DataOutput out) throws IOException {

		seg.write(out);
		out.writeInt(splits.length);
		ByteBuffer bbuffer = ByteBuffer.allocate(splits.length * 8);
		for (double split : splits)
			bbuffer.putDouble(split);
		if (bbuffer.hasRemaining())
			throw new RuntimeException("Did not calculate buffer size correctly");
		out.write(bbuffer.array(), bbuffer.arrayOffset(), bbuffer.position());

	}

	public void readFields(DataInput in) throws IOException {

		seg.readFields(in);
		int partitions = in.readInt();
		splits = new double[partitions];

		int bufferLength = splits.length * 8;
		byte[] buffer = new byte[bufferLength];
		in.readFully(buffer);
		ByteBuffer bbuffer = ByteBuffer.wrap(buffer);
		for (int i = 0; i < splits.length; i++)
			splits[i] = bbuffer.getDouble();
		if (bbuffer.hasRemaining())
			throw new RuntimeException("Error reading BPT partitioner");

	}

	public double log2(double N) {
		return Math.log(N) / Math.log(2);// Math.log的底为e
	}

	@Override
	public void createFromPoints(Segment mbr, IPoint[] points, int capacity) throws IllegalArgumentException {

		// Enumerate all partition IDs to be able to count leaf nodes in any split
		// TODO do the same functionality without enumerating all IDs
		// System.out.println(points.length);
		int numSplits = (int) Math.ceil((double) points.length / capacity);
		System.out.println("分区数：" + numSplits);
		// 定义树的高度
		int h = (int) Math.ceil(log2(numSplits)) + 1;
		System.out.println("树高：" + h);
		String[] ids = new String[numSplits];
		for (int id = numSplits; id < 2 * numSplits; id++) {
			ids[id - numSplits] = Integer.toBinaryString(id);
		}
		// Keep splitting the 1D space into halves until we reach the desired number of
		// partitions

		Comparator<IPoint> comparators = new Comparator<IPoint>() {
			public int compare(IPoint a, IPoint b) {
				// 从大到小排序
				return a.x < b.x ? -1 : (a.x > b.x ? 1 : 0);
			}
		};

		class SplitTask {
			int fromIndex;
			int toIndex;
			// int direction;
			int partitionID;

			/** Constructor using all fields */
			public SplitTask(int fromIndex, int toIndex, int partitionID) {
				this.fromIndex = fromIndex;
				this.toIndex = toIndex;
				// this.direction = direction;
				this.partitionID = partitionID;
			}
		}
		Queue<SplitTask> splitTasks = new ArrayDeque<SplitTask>();
		splitTasks.add(new SplitTask(0, points.length, 1));
		this.seg.set(mbr);
		this.splits = new double[numSplits];
		// map集合保存左奇异值的分区ID和奇异值
		// Map<Integer, Integer> l_map = new HashMap<Integer, Integer>();
		Map<Integer, Double> lodd_map = new HashMap<Integer, Double>();
		// map集合保存所有分区ID和索引下标
		Map<Integer, Integer> normal_map = new HashMap<Integer, Integer>();
		while (!splitTasks.isEmpty()) {
			SplitTask splitTask = splitTasks.remove();
			if (splitTask.partitionID < numSplits) {
				String child1 = Integer.toBinaryString(splitTask.partitionID * 2);
				String child2 = Integer.toBinaryString(splitTask.partitionID * 2 + 1);
				int size_child1 = 0, size_child2 = 0;
				for (int i = 0; i < ids.length; i++) {
					if (ids[i].startsWith(child1))
						size_child1++;
					else if (ids[i].startsWith(child2))
						size_child2++;
				}
				// Calculate the index which partitions the subrange into sizes proportional to
				// size_child1 and size_child2
				int splitIndex = (int) (((long) size_child1 * splitTask.toIndex
						+ (long) size_child2 * splitTask.fromIndex) / (size_child1 + size_child2));
				// System.out.println(
				// points.length + "," + splitTask.fromIndex + "," + splitTask.toIndex + "," +
				// splitIndex);
				if (splitTask.partitionID == 1) {
					partialQuickSort(points, splitTask.fromIndex, splitTask.toIndex, splitIndex, comparators);
				}
				IPoint splitValue = points[splitIndex];
				normal_map.put(splitTask.partitionID, splitIndex);
				IPoint left_splitValue = points[splitIndex - 1];
				if (left_splitValue.x == splitValue.x) {
					// System.out.println(splitValue.x + "是左奇异值！");
					lodd_map.put(splitTask.partitionID, splitValue.x);
				}
				this.splits[splitTask.partitionID] = splitValue.x;
				// System.out.println(splitTask.partitionID + "---" + splitValue.x);
				splitTasks.add(new SplitTask(splitTask.fromIndex, splitIndex, splitTask.partitionID * 2));
				splitTasks.add(new SplitTask(splitIndex, splitTask.toIndex, splitTask.partitionID * 2 + 1));
			}
		}

		// 寻找左奇异数
		int tmp = 0;
		int left_partitionID = 0;
		for (Integer partitionID : lodd_map.keySet()) {
			// int level = getNumberOfSignificantBits(partitionID);
			if (partitionID * 2 <= numSplits - 1) {
				tmp = (partitionID << 2) + 1;
				while (tmp <= numSplits - 1) {
					// left_partitionID = tmp;
					tmp <<= 1;
					tmp += 1;
				}
				left_partitionID = tmp - 1 >> 1;
			} else {
				// 根据ID计算层数
				int level = getNumberOfSignificantBits(partitionID);
				int i = 1;
				if ((int) Math.pow(2, level - 1) != partitionID) {
					if (partitionID % 2 == 0) {
						String binaryString = Integer.toBinaryString(partitionID);
						while (i <= level) {
							if ((partitionID >> i) % 2 != 0) {
								break;
							} else {
								i++;
							}
						}
						String substring = binaryString.substring(0, binaryString.length() - i - 1);
						left_partitionID = Integer.parseInt(substring, 2);
					} else {
						left_partitionID = partitionID / 2;
					}
				} else {
					left_partitionID = Integer.MAX_VALUE;
				}
			}
			// System.out.println(partitionID + "===" + left_partitionID + "---");
			int bsplitIndex = normal_map.get(left_partitionID) == null ? 0 : normal_map.get(left_partitionID);
			int esplitIndex = normal_map.get(partitionID);
			double odd_value = lodd_map.get(partitionID);
			int index = bsplitIndex;
			while (index < esplitIndex) {
				if (points[index].equals(new IPoint(odd_value))) {
					break;
				}
				index++;
			}
			// 查找范围内奇异值个数
			int odd_nums = esplitIndex - index;
			lmap.put(partitionID, odd_nums);
		}
		for (Integer splitID : lmap.keySet()) {
			Double splitValue = lodd_map.get(splitID);
			if (splitID * 2 <= numSplits - 1) {
				int numberOfSignificantBits = getNumberOfSignificantBits(splitID * 2);
				String binaryString = Integer.toBinaryString(splitID * 2);
				for (int id = splitID * 2; id <= numSplits - 1; id++) {
					if (lodd_map.containsKey(id) && splitValue.doubleValue() == lodd_map.get(id)
							&& Integer.toBinaryString(id).substring(0, numberOfSignificantBits).equals(binaryString)) {
						lmap.put(splitID, lmap.get(splitID) + lmap.get(id));
					}
				}

			}
		}
		// lmap.put(4, lmap.get(splitID) + lmap.get(id));
		// for (Integer splitID : lmap.keySet()) {
		// System.out.println(splitID + "----" + lmap.get(splitID));
		// }
	}

	public static <T> void partialQuickSort(T[] a, int fromIndex, int toIndex, int desiredIndex, Comparator<T> c) {
		Arrays.sort(a, fromIndex, toIndex, c);
	}

	@Override
	public void overlapPartitions(Shape shape, ResultCollector<Integer> matcher) {

		if (shape == null || shape.getSegment() == null)
			return;
		Segment shapeMBR = shape.getSegment();
		/** Information about a split to test */
		class SplitToTest {
			/** The ID of the split in the array of splits in the KDTreePartitioner */
			int splitID;

			/** Direction of the split. 0 is vertical (|) and 1 is horizontal (-) */
			// int direction;

			public SplitToTest(int splitID) {
				this.splitID = splitID;
				// this.direction = direction;
			}
		}
		// A queue of all splits to test
		Queue<SplitToTest> splitsToTest = new ArrayDeque<SplitToTest>();
		// Start from the first (root) split
		splitsToTest.add(new SplitToTest(1));

		while (!splitsToTest.isEmpty()) {
			SplitToTest splitToTest = splitsToTest.remove();
			if (splitToTest.splitID >= splits.length) {
				// Matched a partition. return it
				matcher.collect(splitToTest.splitID);
			} else {
				// Need to test that split

				// The corresponding split is vertical (along the x-axis). Like |
				if (shapeMBR.x1 < splits[splitToTest.splitID])
					splitsToTest.add(new SplitToTest(splitToTest.splitID * 2)); // Go left
				if (shapeMBR.x2 > splits[splitToTest.splitID])
					splitsToTest.add(new SplitToTest(splitToTest.splitID * 2 + 1)); // Go right

			}
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.dlmu.wisdomST.indexing.Partitioner#overlapPartition1D(com.dlmu.wisdomST.
	 * core.Shape1D)
	 */
	@Override
	public int overlapPartition(Shape shape) {
		if (shape == null || shape.getSegment() == null)
			return -1;
		IPoint pt = shape.getSegment().getCenterPoint();
		int splitID = 1; // Start from the root
		// int direction = 0;
		while (splitID < splits.length) {
			// The corresponding split is vertical (along the x-axis). Like |
			if (pt.x < splits[splitID]) {
				splitID = splitID * 2; // Go left
			} else if (pt.x == splits[splitID] && lmap.containsKey(splitID) && lmap.get(splitID) != 0) {
				lmap.put(splitID, lmap.get(splitID) - 1);
				splitID = splitID * 2; // Go left
			} else {
				splitID = splitID * 2 + 1;
			}
		}
		return splitID;

	}

	public CellInfo getPartition(int id) {

		CellInfo cellInfo = new CellInfo(id, seg);
		boolean minXFound = false, maxXFound = false;
		// Direction 0 means x-axis and 1 means y-axis
		// int direction = getNumberOfSignificantBits(id) & 1;
		while (id > 1) {
			// 0 means maximum and 1 means minimum
			int minOrMax = id & 1;
			id >>>= 1;
			if (minOrMax == 0 && !maxXFound) {
				cellInfo.x2 = splits[id];
				maxXFound = true;
			} else if (minOrMax == 1 && !minXFound) {
				cellInfo.x1 = splits[id];
				minXFound = true;
			}

		}
		return cellInfo;

	}

	public CellInfo getPartitionAt(int index) {
		// TODO Auto-generated method stub
		return getPartition(index + splits.length);
	}

	@Override
	public int getPartitionCount() {
		// TODO Auto-generated method stub
		return splits.length;
	}

	public static int getNumberOfSignificantBits(int x) {
		int numOfSignificantBits = 0;
		if ((x & 0xffff0000) != 0) {
			// There's some bit on the upper word that is not zero
			numOfSignificantBits += 16;
			x >>>= 16;
		}
		if ((x & 0xff00) != 0) {
			// There's some non-zero bit in the upper byte
			numOfSignificantBits += 8;
			x >>>= 8;
		}
		if ((x & 0xf0) != 0) {
			numOfSignificantBits += 4;
			x >>>= 4;
		}
		if ((x & 0xC) != 0) {
			numOfSignificantBits += 2;
			x >>>= 2;
		}
		if ((x & 0x2) != 0) {
			numOfSignificantBits += 1;
			x >>>= 1;
		}
		if ((x & 0x1) != 0) {
			numOfSignificantBits += 1;
			// id >>>= 1; // id will always be zero
		}
		return numOfSignificantBits;
	}

	public static void main(String[] args) throws IOException {
		args = new String[2];
		args[0] = "hdfs://localhost:9000/demo/test.txt";
		args[1] = "shape:com.dlmu.BOD_tree.core.TrajBus4";
		// args[2] = "segment:90000156,90000168";
		OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
		Path inPath = params.getInputPath();

		long length = inPath.getFileSystem(params).getFileStatus(inPath).getLen();
		ShapeIterRecordReader reader = new ShapeIterRecordReader(params,
				new FileSplit(inPath, 0, length, new String[0]));
		Segment key = reader.createKey();
		ShapeIterator shapes = reader.createValue();
		final Vector<IPoint> points = new Vector<IPoint>();
		while (reader.next(key, shapes)) {
			for (Shape s : shapes) {
				points.add(s.getSegment().getCenterPoint());
			}
		}
		// System.out.println(points.size() + "----");
		Segment inMBR = (Segment) OperationsParams.getShape(params, "segment");
		// System.out.println(inMBR);
		BOD_tree bpt = new BOD_tree();
		bpt.createFromPoints(inMBR, points.toArray(new IPoint[points.size()]), 200);

		int[] sizes = new int[bpt.getPartitionCount() * 2];
		for (IPoint p : points) {
			int partition = bpt.overlapPartition(p);
			sizes[partition]++;
		}
		for (int i = sizes.length / 2; i < sizes.length; i++) {
			System.out.println(i + "-aaaa--" + sizes[i]);

		}
	}

}
