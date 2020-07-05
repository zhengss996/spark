/**
 * ORCFormat.java
 * com.hainiu.mapreducer.util
 * Copyright (c) 2018, 海牛版权所有.
 * @author   青牛
*/

package util;

/**
 * ORC表格式
 */
public class ORCFormat {

	/**
	 * hive仓储user_install_status表的数据格式
	 */
	public static final String INS_STATUS="struct<aid:string,pkgname:string,uptime:bigint,type:int,country:string,gpcategory:string>";

}

