/****************************************************************************
*
* Copyright© 2021 SEI of the Dalian Maritime University. All Rights Reserved
*
*                   大连海事大学软件工程研究所 版权所有
*
****************************************************************************/
package com.dlmu.BOD_tree.util;

import java.awt.Color;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;

import com.dlmu.BOD_tree.core.IPoint;
import com.dlmu.BOD_tree.core.Shape;
import com.dlmu.BOD_tree.core.SpatialSite;
import com.dlmu.BOD_tree.io.TextSerializable;

/**
 * A class that encapsulates all parameters sent for an operations implemented
 * in SpatialHadoop. Internally, it stores everything in {@link Configuration}
 * which makes it easier to pass around to mappers and reducers. It can be
 * initialized from a configuration and/or a list of command line arguments.
 * 
 * @author 田瑞杰
 *
 */
public class OperationsParams extends Configuration {
	private static final Log LOG = LogFactory.getLog(OperationsParams.class);

	/** Separator between shape type and value */
	public static final String ShapeValueSeparator = "//";

	/** Maximum number of splits to handle by a local algorithm */
	private static final int MaxSplitsForLocalProcessing = Runtime.getRuntime().availableProcessors();

	private static final long MaxSizeForLocalProcessing = 200 * 1024 * 1024;

	/** All detected input paths */
	private Path[] allPaths;

	static {
		// Load configuration from files
		Configuration.addDefaultResource("spatial-default.xml");
		Configuration.addDefaultResource("spatial-site.xml");
	}

	public OperationsParams() {
		this(new Configuration());
	}

	public OperationsParams(GenericOptionsParser parser) {
		this(parser, true);
	}

	public OperationsParams(GenericOptionsParser parser, boolean autodetectShape) {
		super(parser.getConfiguration());
		initialize(parser.getRemainingArgs());
		if (autodetectShape) {
			TextSerializable shape = getShape("shape");
			if (shape != null) {
				// In case this class is in a third part jar file, add it to
				// path
				SpatialSite.addClassToPath(this, shape.getClass());
			}
		}
	}

	/**
	 * Initialize the command line arguments from an existing configuration and a
	 * list of additional arguments.
	 * 
	 * @param conf
	 *            A set of configuration parameters to initialize to
	 * @param additionalArgs
	 *            Any additional command line arguments given by the user.
	 */
	public OperationsParams(Configuration conf, String... additionalArgs) {
		super(conf);
		initialize(additionalArgs);
	}

	public OperationsParams(OperationsParams params) {
		super(params);
		if (params.allPaths != null)
			this.allPaths = params.allPaths.clone();
	}

	public void initialize(String... args) {
		// TODO if the argument shape is set to a class in a third party jar
		// file add that jar file to the archives
		Vector<Path> paths = new Vector<Path>();
		for (String arg : args) {
			String argl = arg.toLowerCase();
			if (arg.startsWith("-no-")) {
				this.setBoolean(argl.substring(4), false);
			} else if (argl.startsWith("-")) {
				this.setBoolean(argl.substring(1), true);
			} else if (argl.contains(":") && !argl.contains(":/")) {
				String[] parts = arg.split(":", 2);
				String key = parts[0].toLowerCase();
				String value = parts[1];
				String previousValue = this.get(key);
				if (previousValue == null)
					this.set(key, value);
				else
					this.set(key, previousValue + "\n" + value);
			} else if (argl.contains(":/")) {
				paths.add(new Path(arg));
			}
		}
		this.allPaths = paths.toArray(new Path[paths.size()]);
	}

	public Path[] getPaths() {
		return allPaths;
	}

	public Path getPath() {
		return allPaths.length > 0 ? allPaths[0] : null;
	}

	public Path getOutputPath() {
		return allPaths.length > 1 ? allPaths[allPaths.length - 1] : null;
	}

	public void setOutputPath(String newPath) {
		if (allPaths.length > 1) {
			allPaths[allPaths.length - 1] = new Path(newPath);
		}
	}

	public Path getInputPath() {
		return getPath();
	}

	public Path[] getInputPaths() {
		if (allPaths.length < 2) {
			return allPaths;
		}
		Path[] inputPaths = new Path[allPaths.length - 1];
		System.arraycopy(allPaths, 0, inputPaths, 0, inputPaths.length);
		return inputPaths;
	}

	// public CellInfo[] getCells() {
	// String cell_of = (String) get("cells-of");
	// if (cell_of == null)
	// return null;
	// Path path = new Path(cell_of);
	// FileSystem fs;
	// try {
	// fs = path.getFileSystem(new Configuration());
	// return SpatialSite.cellsOf(fs, path);
	// } catch (IOException e) {
	// e.printStackTrace();
	// }
	// return null;
	// }

	/**
	 * Checks that there is at least one input path and that all input paths exist.
	 * It treats all user-provided paths as input. This is useful for input-only
	 * operations which do not take any output path (e.g., MBR and Aggregate).
	 * 
	 * @return <code>true</code> if there is at least one input path and all input
	 *         paths exist.
	 * @throws IOException
	 */
	public boolean checkInput() throws IOException {
		Path[] inputPaths = getPaths();
		// System.out.println("inputPaths"+inputPaths);
		if (inputPaths.length == 0) {
			LOG.error("No input files");
			return false;
		}

		for (Path path : inputPaths) {
			// Skip existence checks for wild card input
			if (isWildcard(path)) {
				// System.out.println("+++++++++");
				continue;
			}

			FileSystem fs = path.getFileSystem(this);
			if (!fs.exists(path)) {
				// System.out.println(path + "----------");
				LOG.error("Input file '" + path + "' does not exist");
				return false;
			}
		}

		return true;
	}

	public boolean checkInputOutput() throws IOException {
		return checkInputOutput(false);
	}

	/**
	 * Makes standard checks for input and output files. It is assumes that all
	 * files are input files while the last one is the output file. First, it checks
	 * that there is at least one input file. Then, it checks that every input file
	 * exists. After that, it checks for output file, if it exists and the overwrite
	 * flag is not present, it fails.
	 * 
	 * @return <code>true</code> if all checks pass. <code>false</code> otherwise.
	 * @throws IOException
	 *             If the method could not get the information of the input
	 */
	public boolean checkInputOutput(boolean outputRequired) throws IOException {
		Path[] inputPaths = getInputPaths();
		if (inputPaths.length == 0) {
			LOG.error("No input files");
			return false;
		}
		for (Path path : inputPaths) {
			// System.out.println(path + "============");
			if (isWildcard(path))
				continue;
			FileSystem fs = path.getFileSystem(this);
			// System.out.println(fs.exists(path) + "============");
			if (!fs.exists(path)) {
				LOG.error("Input file '" + path + "' does not exist");
				return false;
			}
		}
		Path outputPath = getOutputPath();
		if (outputPath == null && outputRequired) {
			LOG.error("Output path is missing");
			return false;
		}
		if (outputPath != null) {
			FileSystem fs = outputPath.getFileSystem(this);
			if (fs.exists(outputPath)) {
				if (this.getBoolean("overwrite", false)) {
					fs.delete(outputPath, true);
				} else {
					LOG.error("Output file '" + outputPath + "' exists and overwrite flag is not set");
					return false;
				}
			}
		}
		return true;
	}

	public boolean checkOutput() throws IOException {
		Path[] inputPaths = getInputPaths();
		Path outputPath = inputPaths[inputPaths.length - 1];
		if (outputPath != null) {
			FileSystem fs = outputPath.getFileSystem(this);
			if (fs.exists(outputPath)) {
				if (this.getBoolean("overwrite", false)) {
					fs.delete(outputPath, true);
				} else {
					LOG.error("Output file '" + outputPath + "' exists and overwrite flag is not set");
					return false;
				}
			}
		}
		return true;
	}

	public static boolean isWildcard(Path path) {
		return path.toString().indexOf('*') != -1 || path.toString().indexOf('?') != -1;
	}

	public Color getColor(String key, Color defaultValue) {
		return getColor(this, key, defaultValue);
	}

	public static Color getColor(Configuration conf, String key, Color defaultValue) {
		String colorName = conf.get(key);
		if (colorName == null)
			return defaultValue;

		colorName = colorName.toLowerCase();
		Color color = defaultValue;
		if (colorName.equals("red")) {
			color = Color.RED;
		} else if (colorName.equals("pink")) {
			color = Color.PINK;
		} else if (colorName.equals("blue")) {
			color = Color.BLUE;
		} else if (colorName.equals("cyan")) {
			color = Color.CYAN;
		} else if (colorName.equals("green")) {
			color = Color.GREEN;
		} else if (colorName.equals("black")) {
			color = Color.BLACK;
		} else if (colorName.equals("white")) {
			color = Color.WHITE;
		} else if (colorName.equals("gray")) {
			color = Color.GRAY;
		} else if (colorName.equals("yellow")) {
			color = Color.YELLOW;
		} else if (colorName.equals("orange")) {
			color = Color.ORANGE;
		} else if (colorName.equals("none")) {
			color = new Color(0, 0, 255, 0);
		} else if (colorName.matches("#[a-zA-Z0-9]{8}")) {
			String redHex = colorName.substring(1, 2);
			String greenHex = colorName.substring(3, 4);
			String blueHex = colorName.substring(5, 6);
			String opacityHex = colorName.substring(7, 8);
			int red = Integer.parseInt(redHex, 16);
			int green = Integer.parseInt(greenHex, 16);
			int blue = Integer.parseInt(blueHex, 16);
			int opacity = Integer.parseInt(opacityHex, 16);
			color = new Color(red, green, blue, opacity);
		} else {
			LOG.warn("Does not understand the color '" + conf.get(key) + "'");
		}

		return color;
	}

	public Shape getShape(String key, Shape defaultValue) {

		return getShape(this, key, defaultValue);
	}

	public Shape getShape(String key) {
		return getShape(key, null);
	}

	public static Shape getShape(Configuration job, String key) {
		return getShape(job, key, null);
	}

	public static Shape getShape(Configuration conf, String key, Shape defaultValue) {
		TextSerializable t = getTextSerializable(conf, key, defaultValue);
		return t instanceof Shape ? (Shape) t : null;
	}

	public static TextSerializable getTextSerializable(Configuration conf, String key, TextSerializable defaultValue) {
		String shapeType = conf.get(key);
		if (shapeType == null)
			return defaultValue;

		int separatorIndex = shapeType.indexOf(ShapeValueSeparator);
		Text shapeValue = null;
		if (separatorIndex != -1) {
			shapeValue = new Text(shapeType.substring(separatorIndex + ShapeValueSeparator.length()));
			shapeType = shapeType.substring(0, separatorIndex);
		}

		TextSerializable shape;

		try {
			Class<? extends TextSerializable> shapeClass = conf.getClassByName(shapeType)
					.asSubclass(TextSerializable.class);
			shape = shapeClass.newInstance();
		} catch (Exception e) {
			// shapeClass is not an explicit class name
			String shapeTypeI = shapeType.toLowerCase();
			System.out.println(shapeTypeI);

			// Couldn't detect shape from short name or full class name
			// May be it's an actual value that we can parse

			if (shapeType.split(",").length == 1) {
				// A rectangle
				shape = new IPoint();
				shape.fromText(new Text((String) conf.get(key)));
			} else {
				LOG.warn("unknown shape type: '" + conf.get(key) + "'");
				return null;
			}

		}

		if (shapeValue != null)
			shape.fromText(shapeValue);
		// Special case for CSVOGC shape, specify the column if possible

		return shape;
	}

	public <S extends Shape> S[] getShapes(String key, S stock) {
		String[] values = getArray(key);
		if (values == null)
			return null;
		S[] shapes = (S[]) Array.newInstance(stock.getClass(), values.length);
		for (int i = 0; i < values.length; i++) {
			shapes[i] = (S) stock.clone();
			shapes[i].fromText(new Text(values[i]));
		}
		return shapes;
	}

	private String[] getArray(String key) {
		String val = get(key);
		return val == null ? null : val.split("\n");
	}

	public long getSize(String key) {
		return getSize(this, key);
	}

	/**
	 * Sets the specified configuration parameter to the current value of the shape.
	 * Both class name and shape values are encoded in one string and set as the
	 * value of the configuration parameter. The shape can be retrieved later using
	 * {@link SpatialSite#getShape(Configuration, String)}.
	 * 
	 * @param conf
	 *            The configuration to update
	 * @param param
	 *            The name of the configuration line to set
	 * @param shape
	 *            An object to set in the configuration
	 */
	public static void setShape(Configuration conf, String param, Shape shape) {
		String str = shape.getClass().getName() + ShapeValueSeparator;
		str += shape.toText(new Text()).toString();
		conf.set(param, str);
	}

	public static long getSize(Configuration conf, String key) {
		String size_str = conf.get(key);
		if (size_str == null)
			return 0;
		if (size_str.indexOf('.') == -1)
			return Long.parseLong(size_str);
		String[] size_parts = size_str.split("\\.", 2);
		long size = Long.parseLong(size_parts[0]);
		size_parts[1] = size_parts[1].toLowerCase();
		if (size_parts[1].startsWith("k"))
			size *= 1024;
		else if (size_parts[1].startsWith("m"))
			size *= 1024 * 1024;
		else if (size_parts[1].startsWith("g"))
			size *= 1024 * 1024 * 1024;
		else if (size_parts[1].startsWith("t"))
			size *= 1024 * 1024 * 1024 * 1024;
		return size;
	}

	public static int getJoiningThresholdPerOnce(Configuration conf, String key) {
		String joiningThresholdPerOnce_str = conf.get(key);
		if (joiningThresholdPerOnce_str == null)
			LOG.error("Your joiningThresholdPerOnce is not set");
		return Integer.parseInt(joiningThresholdPerOnce_str);
	}

	public static void setJoiningThresholdPerOnce(Configuration conf, String param, int joiningThresholdPerOnce) {
		String str;
		if (joiningThresholdPerOnce < 0) {
			str = "50000";
		} else {
			str = joiningThresholdPerOnce + "";
		}
		conf.set(param, str);
	}

	public static void setFilterOnlyModeFlag(Configuration conf, String param, boolean filterOnlyMode) {
		String str;
		if (filterOnlyMode) {
			str = "true";
		} else {
			str = "false";
		}
		conf.set(param, str);
	}

	public static boolean getFilterOnlyModeFlag(Configuration conf, String key) {
		String filterOnlyModeFlag = conf.get(key);
		if (filterOnlyModeFlag == null)
			LOG.error("Your filterOnlyMode is not set");
		return Boolean.parseBoolean(filterOnlyModeFlag);
	}

	public static boolean getInactiveModeFlag(Configuration conf, String key) {
		String inactiveModeFlag_str = conf.get(key);
		if (inactiveModeFlag_str == null)
			LOG.error("Your inactiveModeFlag is not set");
		return Boolean.parseBoolean(inactiveModeFlag_str);
	}

	public static void setInactiveModeFlag(Configuration conf, String param, boolean inactiveModeFlag) {
		String str;
		if (inactiveModeFlag) {
			str = "true";
		} else {
			str = "false";
		}
		conf.set(param, str);
	}

	public static Path getRepartitionJoinIndexPath(Configuration conf, String key) {
		String repartitionJoinIndexPath_str = conf.get(key);
		if (repartitionJoinIndexPath_str == null)
			LOG.error("Your index file is not set");
		return new Path(repartitionJoinIndexPath_str);
	}

	public static void setRepartitionJoinIndexPath(Configuration conf, String param, Path indexPath) {
		String str = indexPath.toString();
		conf.set(param, str);
	}

	/** Data type for the direction of skyline to compute */
	public enum Direction {
		MaxMax, MaxMin, MinMax, MinMin
	};

	public Direction getDirection(String key, Direction defaultDirection) {
		return getDirection(this, key, defaultDirection);
	}

	public static Direction getDirection(Configuration conf, String key, Direction defaultDirection) {
		String strdir = conf.get("dir");
		if (strdir == null)
			return defaultDirection;
		Direction dir;
		if (strdir.equalsIgnoreCase("maxmax") || strdir.equalsIgnoreCase("max-max")) {
			dir = Direction.MaxMax;
		} else if (strdir.equalsIgnoreCase("maxmin") || strdir.equalsIgnoreCase("max-min")) {
			dir = Direction.MaxMin;
		} else if (strdir.equalsIgnoreCase("minmax") || strdir.equalsIgnoreCase("min-max")) {
			dir = Direction.MinMax;
		} else if (strdir.equalsIgnoreCase("minmin") || strdir.equalsIgnoreCase("min-min")) {
			dir = Direction.MinMin;
		} else {
			System.err.println("Invalid direction: " + strdir);
			System.err.println("Valid directions are: max-max, max-min, min-max, and min-min");
			return null;
		}
		return dir;
	}

	/**
	 * Auto detect shape type based on input format
	 * 
	 * @return <code>true</code> if a shape is already present in the configuration
	 *         or it is auto detected from input files. <code>false</code> if it was
	 *         not set and could not be detected.
	 */

	/**
	 * Detects the shape of the given set of files assuming all of them hold data of
	 * the same shape. It reads a random set of lines and uses the
	 * {@link #detectShape(String[])} method to detect the shape of this sample set
	 * of lines.
	 * 
	 * @param path
	 *            Input path to read data from
	 * @param conf
	 *            The configuration parameters of the environment
	 * @return The detected type of the shape or <code>null</code> if failed.
	 * @throws IOException
	 * @throws InterruptedException
	 */

	/**
	 * Detects the shape from a sample set of lines
	 * 
	 * @param lines
	 * @return
	 */

	/**
	 * Checks whether the operation should work in local or MapReduce mode. If the
	 * job explicitly specifies whether to run in local or MapReduce mode, the
	 * specified option is returned. Otherwise, it automatically detects whether to
	 * use local or MapReduce based on the input size.
	 * 
	 * @return <code>true</code> to run in local mode, <code>false</code> to run in
	 *         MapReduce mode.
	 * @throws IOException
	 *             If the underlying job fails with an IOException
	 * @throws InterruptedException
	 *             If the underlying job was interrupted
	 */

	public void clearAllPaths() {
		this.allPaths = null;
	}

	/**
	 * @param queryParams
	 * @param string
	 * @param string2
	 */
	public static void setCarId(Configuration conf, String param, String id) {
		conf.set(param, id);

	}

	public static String getCarId(Configuration conf, String param) {
		return conf.get(param);

	}
}
