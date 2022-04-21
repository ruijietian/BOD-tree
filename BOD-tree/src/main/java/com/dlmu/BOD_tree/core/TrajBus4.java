/***********************************************************************
*
* Copyright© 2021 SEI of the Dalian Maritime University. All Rights Reserved
*
*                   大连海事大学软件工程研究所 版权所有
*
*************************************************************************/
package com.dlmu.BOD_tree.core;

import java.awt.Graphics;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;



/**
 * A data type used for GeoLife trajectories dataset.
 *  Metadata: uerID-Tripid,0,Altitude,FractionNumber,DateTime,Longitude,Latitude
	Example: 102-20111023101803,0,0,40839.4303703704,2011-10-23 10:19:44,39.9067083333333,116.429903333333
 * @author 田瑞杰
 * 
 */
public class TrajBus4 extends TIPoint{
	private static final Log LOG = LogFactory.getLog(TrajBus4.class);
	public String pts_id;
	public String pts_carid;
	public String pts_devid;
	public String pts_lon;
	public String pts_lat;
	public String pts_speed;
	public String pts_heading;
	public String pts_workstatus;
	public String pts_status;
	public String pts_sumcourse;
	public String pts_course;
	public String pts_line;
	public String pts_stationid;
	public String pts_driverno;
	public String pts_type;
	public String pts_sublineno;
	public String pts_datetime;
	public String road;

	public TrajBus4() {
		// TODO Auto-generated constructor stub
	}

	public TrajBus4(String text) throws ParseException {
		String[] list = text.toString().split(",");
		pts_id = list[0];
		pts_carid = list[1];
		pts_devid=list[2];
		pts_lon = list[3];
		pts_lat = list[4];
		pts_speed = list[5];
		pts_heading = list[6];
		pts_workstatus = list[7];
		pts_status= list[8];
		pts_sumcourse= list[9];
		pts_course = list[10];
		pts_line = list[11];
		pts_stationid = list[12];
		pts_driverno= list[13];
		pts_type= list[14];
		pts_sublineno= list[15];
		pts_datetime = list[16];
		road = list[17];
		//System.out.println("TrajBus--------");
		//System.out.println(id + "," + carid + ","+speed + ","+ heading + "," +workstatus+","+ course + "," + lineno + "," + stationid+","+datetime+ "," + longitude + "," +latitude);
		super.fromText(new Text(pts_datetime+","+road));

	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(pts_id);
		out.writeUTF(pts_carid);
		out.writeUTF(pts_devid);
		out.writeUTF(pts_lon);
		out.writeUTF(pts_lat);
		out.writeUTF(pts_speed);
		out.writeUTF(pts_heading);
		out.writeUTF(pts_workstatus);
		out.writeUTF(pts_status);
		out.writeUTF(pts_sumcourse);
		out.writeUTF(pts_course);
		out.writeUTF(pts_line);
		out.writeUTF(pts_stationid);
		out.writeUTF(pts_driverno);
		out.writeUTF(pts_type);
		out.writeUTF(pts_sublineno);
		super.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {	
		
		pts_id = in.readUTF();
		pts_carid = in.readUTF();
		pts_devid=in.readUTF();
		pts_lon = in.readUTF();
		pts_lat = in.readUTF();
		pts_speed = in.readUTF();
		pts_heading = in.readUTF();
		pts_workstatus = in.readUTF();
		pts_status= in.readUTF();
		pts_sumcourse=in.readUTF();
		pts_course = in.readUTF();
		pts_line = in.readUTF();
		pts_stationid = in.readUTF();
		pts_driverno= in.readUTF();
		pts_type= in.readUTF();
		pts_sublineno= in.readUTF();
		super.readFields(in);

	}

	@Override
	public Text toText(Text text) {
		byte[] separator = new String(",").getBytes();
		text.append(pts_id.getBytes(), 0, pts_id.getBytes().length);
		text.append(separator, 0, separator.length);
		text.append(pts_carid.getBytes(), 0, pts_carid.getBytes().length);
		text.append(separator, 0, separator.length);
		text.append(pts_devid.getBytes(), 0, pts_devid.getBytes().length);
		text.append(separator, 0, separator.length);
		text.append(pts_lon.getBytes(), 0, pts_lon.getBytes().length);
		text.append(separator, 0, separator.length);
		text.append(pts_lat.getBytes(), 0, pts_lat.getBytes().length);
		text.append(separator, 0, separator.length);
		text.append(pts_speed.getBytes(), 0, pts_speed.getBytes().length);
		text.append(separator, 0, separator.length);
		text.append(pts_heading.getBytes(), 0, pts_heading.getBytes().length);
		text.append(separator, 0, separator.length);
		text.append(pts_workstatus.getBytes(), 0, pts_workstatus.getBytes().length);
		text.append(separator, 0, separator.length);
		text.append(pts_status.getBytes(), 0, pts_status.getBytes().length);
		text.append(separator, 0, separator.length);
		text.append(pts_sumcourse.getBytes(), 0, pts_sumcourse.getBytes().length);
		text.append(separator, 0, separator.length);
		text.append(pts_course.getBytes(), 0, pts_course.getBytes().length);
		text.append(separator, 0, separator.length);
		text.append(pts_line.getBytes(), 0, pts_line.getBytes().length);
		text.append(separator, 0, separator.length);
		text.append(pts_stationid.getBytes(), 0, pts_stationid.getBytes().length);
		text.append(separator, 0, separator.length);
		text.append(pts_driverno.getBytes(), 0, pts_driverno.getBytes().length);
		text.append(separator, 0, separator.length);
		text.append(pts_type.getBytes(), 0, pts_type.getBytes().length);
		text.append(separator, 0, separator.length);
		text.append(pts_sublineno.getBytes(), 0, pts_sublineno.getBytes().length);
		text.append(separator, 0, separator.length);
		super.toText(text);
		return text;
	}

	@Override
	public void fromText(Text text) {
		//System.out.println(text+"fromText--------");
		String[] list = text.toString().split(",");
		pts_id = list[0];
		pts_carid = list[1];
		pts_devid=list[2];
		pts_lon = list[3];
		pts_lat = list[4];
		pts_speed = list[5];
		pts_heading = list[6];
		pts_workstatus = list[7];
		pts_status= list[8];
		pts_sumcourse= list[9];
		pts_course = list[10];
		pts_line = list[11];
		pts_stationid = list[12];
		pts_driverno= list[13];
		pts_type= list[14];
		pts_sublineno= list[15];
		pts_datetime = list[16];
		road = list[17];
		super.fromText(new Text(pts_datetime+","+road));

	}

	@Override
	public TrajBus4 clone() {
		TrajBus4 c = new TrajBus4();
		c.pts_id = this.pts_id;
		c.pts_carid = this.pts_carid;
		c.pts_devid = this.pts_devid;
		c.pts_lon = this.pts_lon;
		c.pts_lat = this.pts_lat;
		c.pts_speed = this.pts_speed;
		c.pts_heading = this.pts_heading;
		c.pts_workstatus = this.pts_workstatus;
		c.pts_status = this.pts_status;
		c.pts_sumcourse = this.pts_sumcourse;
		c.pts_course = this.pts_course;
		c.pts_line = this.pts_line;
		c.pts_stationid = this.pts_stationid;
		c.pts_driverno = this.pts_driverno;
		c.pts_type = this.pts_type;
		c.pts_sublineno = this.pts_sublineno;
		c.pts_datetime = this.pts_datetime;
		c.road=this.road;
		c.time = this.time;
		c.x = this.x;
		return c;
	}

	@Override
	public String toString() {
		return "BusTrajectory: (" + pts_id + "," + pts_carid + ","+pts_speed + ","+ pts_heading + "," +pts_workstatus+","+ pts_course + "," + pts_line + "," + pts_stationid+","+pts_datetime+ "," + x +")";
	}
	public String getTime() {
		return time;
	}

	public static void main(String[] args) {
		String temp = "15316754,1521,15091521,121.486127,38.90323,0,0,11780,-1,491713.21,0.06,90009551,1,2019/4/17 8:46,1400234,68,1,1001";
		TrajBus4 point = new TrajBus4();
		point.fromText(new Text(temp));
		TIPoint point3d = (TIPoint) point;
		
		System.out.println(point.x);
		System.out.println(point3d.x);
		
		
		if (!(point instanceof TIPoint)) {
			LOG.error("Shape is not instance of TIPoint");
			return;
		}
		System.out.println("Point : " + point.toString());
		System.out.println("Point3D : " + point3d.toString());
		

	}

}
