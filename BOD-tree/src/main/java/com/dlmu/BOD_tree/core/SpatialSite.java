/***********************************************************************
*
* Copyright© 2021 SEI of the Dalian Maritime University. All Rights Reserved
*
*                   大连海事大学软件工程研究所 版权所有
*
*************************************************************************/
package com.dlmu.BOD_tree.core;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import com.dlmu.BOD_tree.indexing.GlobalIndex;
import com.dlmu.BOD_tree.operations.ShapeIterRecordReader;
import com.dlmu.BOD_tree.operations.SpatialRecordReader;
import com.dlmu.BOD_tree.util.OperationsParams;

/**
 * Combines all the configuration needed for wisdomST.
 * 
 * @author 田瑞杰
 *
 */
public class SpatialSite {

	private static final Log LOG = LogFactory.getLog(SpatialSite.class);

	/**
	 * A filter that selects visible files and filters out hidden files. Hidden
	 * files are the ones with a names starting in '.' or '_'
	 */
	public static final PathFilter NonHiddenFileFilter = new PathFilter() {
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_") && !name.startsWith(".");
		}
	};

	/**
	 * Configuration line to store column boundaries on which intermediate data is
	 * split
	 */
	public static final String ColumnBoundaries = "SpatialSite.ReduceSpaceBoundaries";

	/** Enforce static only calls */
	private SpatialSite() {
	}

	/** The class used to filter blocks before starting map tasks */
	public static final String FilterClass = "spatialHadoop.mapreduce.filter";

	/** Whether to build the RTree in fast mode or slow (memory saving) mode. */
	public static final String RTREE_BUILD_MODE = "spatialHadoop.storage.RTreeBuildMode";

	/** Configuration line name for replication overhead */
	public static final String INDEXING_OVERHEAD = "spatialHadoop.storage.IndexingOverhead";

	/** Ratio of the sample to read from files to build a global R-tree */
	public static final String SAMPLE_RATIO = "spatialHadoop.storage.SampleRatio";

	/** Ratio of the sample to read from files to build a global R-tree */
	public static final String SAMPLE_SIZE = "spatialHadoop.storage.SampleSize";

	/**
	 * A marker put in the beginning of each block to indicate that this block is
	 * stored as an RTree. It might be better to store this in the BlockInfo in a
	 * field (e.g. localIndexType).
	 */
	public static final long RTreeFileMarker = -0x00012345678910L;

	public static final String OUTPUT_CELLS = "edu.umn.cs.spatial.mapReduce.GridOutputFormat.CellsInfo";
	public static final String OVERWRITE = "edu.umn.cs.spatial.mapReduce.GridOutputFormat.Overwrite";

	private static final CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(new Configuration());

	/**
	 * Maximum number of shapes to read in one read operation and return when
	 * reading a file as array
	 */
	public static final String MaxShapesInOneRead = "spatialHadoop.mapred.MaxShapesPerRead";

	/**
	 * Maximum size in bytes that can be read in one read operation
	 */
	public static final String MaxBytesInOneRead = "spatialHadoop.mapred.MaxBytesPerRead";

	public static byte[] RTreeFileMarkerB;

	static {
		// Load configuration from files
		Configuration.addDefaultResource("spatial-default.xml");
		Configuration.addDefaultResource("spatial-site.xml");

		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(bout);
		try {
			dout.writeLong(RTreeFileMarker);
			dout.close();
			bout.close();
			RTreeFileMarkerB = bout.toByteArray();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * It sets the given class in the configuration and, in addition, it sets the
	 * jar of that class to the class path of this job which allows it to run
	 * correctly in a distributed mode.
	 * 
	 * @param conf
	 *            - Configuration to set the key
	 * @param key
	 *            - the key to set
	 * @param klass
	 *            - the class to use as a value
	 * @param xface
	 *            - the interface that the provided class should implement
	 */
	public static void setClass(Configuration conf, String key, Class<?> klass, Class<?> xface) {
		conf.setClass(key, klass, xface);
		addClassToPath(conf, klass);
	}

	private static String findContainingJar(Class my_class) {
		ClassLoader loader = my_class.getClassLoader();
		String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";
		try {
			for (Enumeration<URL> itr = loader.getResources(class_file); itr.hasMoreElements();) {
				URL url = (URL) itr.nextElement();
				if ("jar".equals(url.getProtocol())) {
					String toReturn = url.getPath();
					if (toReturn.startsWith("file:")) {
						toReturn = toReturn.substring("file:".length());
					}
					toReturn = URLDecoder.decode(toReturn, "UTF-8");
					return toReturn.replaceAll("!.*$", "");
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return null;
	}

	/**
	 * Ensures that the given class is in the class path of running jobs. If the jar
	 * is not already in the class path, it is added to the DisributedCache of the
	 * given job to ensure the associated job will work fine.
	 * 
	 * @param conf
	 * @param klass
	 */
	public static void addClassToPath(Configuration conf, Class<?> klass) {
		// Check if we need to add the containing jar to class path
		String klassJar = findContainingJar(klass);
		String shadoopJar = findContainingJar(SpatialSite.class);
		if (klassJar == null || (shadoopJar != null && klassJar.equals(shadoopJar)))
			return;
		Path containingJar = new Path(findContainingJar(klass));
		Path[] existingClassPaths = DistributedCache.getArchiveClassPaths(conf);
		if (existingClassPaths != null) {
			for (Path existingClassPath : existingClassPaths) {
				if (containingJar.getName().equals(existingClassPath.getName()))
					return;
			}
		}
		// The containing jar is a new one and needs to be copied to class path
		try {
			LOG.info("Adding JAR '" + containingJar.getName() + "' to job class path");
			FileSystem defaultFS = FileSystem.get(conf);
			Path libFolder;
			if (existingClassPaths != null && existingClassPaths.length > 0) {
				libFolder = existingClassPaths[0].getParent();
			} else {
				// First jar to be added like this. Create a new lib folder
				do {
					libFolder = new Path("lib_" + (int) (Math.random() * 100000));
				} while (defaultFS.exists(libFolder));
				defaultFS.mkdirs(libFolder);
				defaultFS.deleteOnExit(libFolder);
			}
			defaultFS.copyFromLocalFile(containingJar, libFolder);
			Path jarFullPath = new Path(libFolder, containingJar.getName()).makeQualified(defaultFS);
			jarFullPath = jarFullPath.makeQualified(defaultFS);
			DistributedCache.addArchiveToClassPath(jarFullPath, conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Creates a stock shape according to the given configuration. It is a shortcut
	 * to {@link #getShape(Configuration, String)} called for this configuration and
	 * with the given parameter name.
	 * 
	 * @param job
	 * @return
	 */
	public static Shape createStockShape(Configuration job) {
		return OperationsParams.getShape(job, "shape");
	}

	/**
	 * Sets the specified configuration parameter to the current value of the shape.
	 * Both class name and shape values are encoded in one string and set as the
	 * value of the configuration parameter. The shape can be retrieved later using
	 * {@link #getShape(Configuration, String)}.
	 * 
	 * @param conf
	 * @param param
	 * @param shape
	 * @deprecated Use {@link OperationsParams#setShape(Configuration,String,Shape)}
	 *             instead
	 */
	public static void setShape(Configuration conf, String param, Shape shape) {
		OperationsParams.setShape(conf, param, shape);
	}

	/**
	 * Retrieves a value of a shape set earlier using
	 * {@link OperationsParams#setShape(Configuration, String, Shape)}. It reads the
	 * corresponding parameter and parses it to find the class name and shape value.
	 * First, a default object is created using {@link Class#newInstance()} then the
	 * value is parsed using {@link Shape#fromText(Text)}.
	 * 
	 * @param conf
	 * @param param
	 * @return
	 * @deprecated - Use {@link OperationsParams#getShape(Configuration, String)}
	 */
	@Deprecated
	public static Shape getShape(Configuration conf, String param) {
		return OperationsParams.getShape(conf, param);
	}

	/**
	 * Returns the global index (partitions) of a file that is indexed using the
	 * index command. If the file is not indexed, it returns null. The return value
	 * is of type {@link GlobalIndex} where the generic parameter is specified as
	 * {@link Partition}.
	 * 
	 * @param fs
	 * @param dir
	 * @return
	 */

	public static GlobalIndex<Partition> getGlobalIndex(FileSystem fs, Path dir) {
		try {
			FileStatus[] allFiles;
			if (OperationsParams.isWildcard(dir)) {
				allFiles = fs.globStatus(dir);
			} else {
				allFiles = fs.listStatus(dir);
			}

			FileStatus masterFile = null;
			int nasaFiles = 0;
			for (FileStatus fileStatus : allFiles) {
				if (fileStatus.getPath().getName().startsWith("_master")) {
					if (masterFile != null)
						throw new RuntimeException("Found more than one master file in " + dir);
					masterFile = fileStatus;
				} else if (fileStatus.getPath().getName().toLowerCase().matches(".*h\\d\\dv\\d\\d.*\\.(hdf|jpg|xml)")) {
					// Handle on-the-fly global indexes imposed from file naming of NASA data
					nasaFiles++;
				}
			}
			if (masterFile != null) {
				ShapeIterRecordReader reader = new ShapeIterRecordReader(fs.open(masterFile.getPath()), 0,
						masterFile.getLen());
				Segment dummy = reader.createKey();
				reader.setShape(new Partition());
				SpatialRecordReader.ShapeIterator values = reader.createValue();
				ArrayList<Partition> partitions = new ArrayList<Partition>();
				while (reader.next(dummy, values)) {
					for (Shape value : values) {
						partitions.add((Partition) value.clone());
					}
				}
				GlobalIndex<Partition> globalIndex = new GlobalIndex<Partition>();
				globalIndex.bulkLoad(partitions.toArray(new Partition[partitions.size()]));
				String extension = masterFile.getPath().getName();
				extension = extension.substring(extension.lastIndexOf('.') + 1);
				// globalIndex.setCompact(GridRecordWriter.PackedIndexes.contains(extension));
				// globalIndex.setReplicated(GridRecordWriter.ReplicatedIndexes.contains(extension));
				return globalIndex;
			} else if (nasaFiles > allFiles.length / 2) {
				// A folder that contains HDF files
				// Create a global index on the fly for these files based on their names
				Partition[] partitions = new Partition[allFiles.length];
				for (int i = 0; i < allFiles.length; i++) {
					final Pattern cellRegex = Pattern.compile(".*(h\\d\\dv\\d\\d).*");
					String filename = allFiles[i].getPath().getName();
					Matcher matcher = cellRegex.matcher(filename);
					Partition partition = new Partition();
					partition.filename = filename;
					if (matcher.matches()) {
						String cellname = matcher.group(1);
						int h = Integer.parseInt(cellname.substring(1, 3));
						int v = Integer.parseInt(cellname.substring(4, 6));
						partition.cellId = v * 36 + h;
						// Calculate coordinates on MODIS Sinusoidal grid
						partition.x1 = h * 10 - 180;
						partition.x2 = partition.x1 + 10;
						// Convert to Latitude Longitude
						double lon1 = partition.x1;
						double lon2 = partition.x1;
						partition.x1 = Math.min(lon1, lon2);
						lon1 = partition.x2;
						lon2 = partition.x2;
						partition.x2 = Math.max(lon1, lon2);
					} else {
						partition.set(-180, 180);
						partition.cellId = allFiles.length + i;
					}
					partitions[i] = partition;
				}
				GlobalIndex<Partition> gindex = new GlobalIndex<Partition>();
				gindex.bulkLoad(partitions);
				return gindex;
			} else {
				return null;
			}
		} catch (IOException e) {
			LOG.info("Error retrieving global index of '" + dir + "'");
			LOG.info(e);
			return null;
		}
	}

	/**
	 * Checks whether a file is indexed using an R-tree or not. This allows an
	 * operation to use the R-tree to speedup the processing if it exists. This
	 * function opens the specified file and reads the first eight bytes which
	 * include the R-tree signature. If the signatures matches with the R-tree
	 * signature, true is returned. Otherwise, false is returned. If the parameter
	 * is a path to a directory, only the first data file in that directory is
	 * tested.
	 * 
	 * @param fs
	 * @param path
	 * @return
	 * @throws IOException
	 */

	/**
	 * Returns the cells (partitions) of a file. This functionality can be useful to
	 * repartition another file using the same partitioning or to draw these
	 * partitions as a high level index. This function reads the master file and
	 * returns all rectangles in it.
	 * 
	 * @param fs
	 * @param path
	 * @return
	 * @throws IOException
	 */
	public static CellInfo[] cellsOf(FileSystem fs, Path path) throws IOException {
		GlobalIndex<Partition> gIndex = getGlobalIndex(fs, path);
		if (gIndex == null)
			return null;
		return cellsOf(gIndex);
	}

	/**
	 * Set an array of cells in the job configuration. As the array might be very
	 * large to store as one value, an alternative approach is used. The cells are
	 * all written to a temporary file, and that file is added to the
	 * DistributedCache of the job. Later on, a call to
	 * {@link #getCells(Configuration)} will open the corresponding file from
	 * DistributedCache and parse cells from that file.
	 * 
	 * @param conf
	 * @param cellsInfo
	 * @throws IOException
	 */
	public static void setCells(Configuration conf, CellInfo[] cellsInfo) throws IOException {
		Path tempFile;
		FileSystem fs = FileSystem.get(conf);
		do {
			tempFile = new Path("cells_" + (int) (Math.random() * 1000000) + ".cells");
		} while (fs.exists(tempFile));
		FSDataOutputStream out = fs.create(tempFile);
		out.writeInt(cellsInfo.length);
		for (CellInfo cell : cellsInfo) {
			cell.write(out);
		}
		out.close();

		fs.deleteOnExit(tempFile);

		DistributedCache.addCacheFile(tempFile.toUri(), conf);
		conf.set(OUTPUT_CELLS, tempFile.getName());
		LOG.info("Partitioning file into " + cellsInfo.length + " cells");
	}

	/**
	 * Retrieves cells that were stored earlier using
	 * {@link #setCells(Configuration, CellInfo[])} This function opens the
	 * corresponding file from DistributedCache and parses jobs from it.
	 * 
	 * @param conf
	 * @return
	 * @throws IOException
	 */
	public static CellInfo[] getCells(Configuration conf) throws IOException {
		CellInfo[] cells = null;
		String cells_file = conf.get(OUTPUT_CELLS);
		if (cells_file != null) {
			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
			for (Path cacheFile : cacheFiles) {
				if (cacheFile.getName().contains(cells_file)) {
					FSDataInputStream in = FileSystem.getLocal(conf).open(cacheFile);

					int cellCount = in.readInt();
					cells = new CellInfo[cellCount];
					for (int i = 0; i < cellCount; i++) {
						cells[i] = new CellInfo();
						cells[i].readFields(in);
					}

					in.close();
				}
			}
		}
		return cells;
	}

	/**
	 * Retrieves a rectangle from configuration parameter. The value is assumed to
	 * be in text format that can be parsed using {@link Rectangle#fromText(Text)}
	 * 
	 * @param conf
	 * @param name
	 * @return
	 */

	/**
	 * Retrieves the distribution type used for generating synthetic data
	 * 
	 * @param job
	 * @param key
	 * @param defaultValue
	 * @return
	 */
	// public static DistributionType getDistributionType(Configuration job,
	// String key, DistributionType defaultValue) {
	// DistributionType type = defaultValue;
	// String strType = job.get(key);
	// if (strType != null) {
	// strType = strType.toLowerCase();
	// if (strType.startsWith("uni"))
	// type = DistributionType.UNIFORM;
	// else if (strType.startsWith("gaus"))
	// type = DistributionType.GAUSSIAN;
	// else if (strType.startsWith("cor"))
	// type = DistributionType.CORRELATED;
	// else if (strType.startsWith("anti"))
	// type = DistributionType.ANTI_CORRELATED;
	// else if (strType.startsWith("circle"))
	// type = DistributionType.CIRCLE;
	// else {
	// type = null;
	// }
	// }
	// return type;
	// }

	/**
	 * Finds the partitioning info used in the given global index. If each cell is
	 * represented as one partition, the MBRs of these partitions are returned. If
	 * one cell is stored in multiple partitions (i.e., multiple files), their MBRs
	 * are combined to produce one MBR for this cell.
	 * 
	 * @param gIndex
	 * @return
	 */
	public static CellInfo[] cellsOf(GlobalIndex<Partition> gIndex) {
		// Find all partitions of the given file. If two partitions have the same
		// cell ID, it indicates that they are two blocks of the same cell. This
		// means they represent one partition and should be merged together.
		// They might have different MBRs as each block has its own MBR according
		// to the data stored in it.
		Map<Integer, CellInfo> cells = new HashMap<Integer, CellInfo>();
		for (Partition p : gIndex) {
			CellInfo cell = cells.get(p.cellId);
			if (cell == null) {
				cells.put(p.cellId, cell = new CellInfo(p));
			} else {
				cell.expand(p);
			}
		}
		return cells.values().toArray(new CellInfo[cells.size()]);
	}

	public static CellInfo getCellInfo(GlobalIndex<Partition> gIndex, int cellID) {
		Map<Integer, CellInfo> cells = new HashMap<Integer, CellInfo>();
		for (Partition p : gIndex) {
			CellInfo cell = cells.get(p.cellId);
			if (cell == null) {
				cells.put(p.cellId, cell = new CellInfo(p));
			} else {
				cell.expand(p);
			}
		}
		return cells.get(cellID);
	}

}
