/***********************************************************************
*
* Copyright© 2021 SEI of the Dalian Maritime University. All Rights Reserved
*
*                   大连海事大学软件工程研究所 版权所有
*
*************************************************************************/
package com.dlmu.BOD_tree.core;


/**
 * Used to collect results of unary operators
 * @author 田瑞杰
 *
 */
public interface ResultCollector<R> {
  public void collect(R r);
}
