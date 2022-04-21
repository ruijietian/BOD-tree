/****************************************************************************
*
* Copyright© 2021 SEI of the Dalian Maritime University. All Rights Reserved
*
*                   大连海事大学软件工程研究所 版权所有
*
****************************************************************************/
package com.dlmu.BOD_tree.io;

import org.apache.hadoop.io.Text;

/**
 * Implementing this interface allows objects to be converted easily to and from
 * a string.
 * 
 * @author 田瑞杰
 *
 */
public interface TextSerializable {
	/**
	 * Store current object as string in the given text appending text already
	 * there.
	 * 
	 * @param text
	 *            The text object to append to.
	 * @return The same text that was passed as a parameter
	 */
	public Text toText(Text text);

	/**
	 * Retrieve information from the given text.
	 * 
	 * @param text
	 *            The text to parse
	 */
	public void fromText(Text text);

}
