package demo.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.log4j.Logger;

import demo.hbase.HBaseLib.HBaseTable;

public class AdminHBaseTable {
	static Logger logger = Logger.getLogger(AdminHBaseTable.class);
	
	public void create(String hbaseTableName, String columnFamily) throws MasterNotRunningException, ZooKeeperConnectionException, IOException
	{
		HBaseTable.create(hbaseTableName.getBytes(), columnFamily.getBytes());
	}
}
