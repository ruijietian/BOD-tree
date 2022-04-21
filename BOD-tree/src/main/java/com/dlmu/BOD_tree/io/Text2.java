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
 * A modified version of Text which is optimized for appends.
 * 
 * @author 田瑞杰
 *
 */
public class Text2 extends Text implements TextSerializable {

	public Text2() {
	}

	public Text2(String string) {
		super(string);
	}

	public Text2(Text utf8) {
		super(utf8);
	}

	public Text2(byte[] utf8) {
		super(utf8);
	}

	public Text toText(Text text) {
		text.append(getBytes(), 0, getLength());
		return text;
	}

	public void fromText(Text text) {
		this.set(text);
	}
}
