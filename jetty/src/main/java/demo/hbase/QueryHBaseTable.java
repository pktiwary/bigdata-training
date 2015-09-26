package demo.hbase;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import demo.hbase.HBaseLib.HBaseTable;

public class QueryHBaseTable {
	static Logger logger = Logger.getLogger(QueryHBaseTable.class);
	
    public ArrayList<String[]> query(String hbaseTableName, String columnFamily, String[] inputColumns) throws IOException {
    	BasicConfigurator.configure();
    	HBaseTable client = null;
    	ArrayList<String[]> res = null;
    	try {
    		client = new HBaseTable(hbaseTableName);
		    logger.info("init successful");
		    ArrayList<byte[]> columns = new ArrayList<byte[]>();
		    for(String col: inputColumns) {
		        columns.add(col.getBytes());
		    }
		    res = client.scan(columnFamily.getBytes(), columns);
		    logger.info("scan successful");
		} finally {
		    if(client != null) client.close();
		}
	    return res;
    }
}
