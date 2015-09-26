package demo.hbase.HBaseLib;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

public class HBaseTable {
	private Configuration config;
	private ExecutorService executor;
	private Connection conn;
	private Table table;
	
	public HBaseTable(String tableName) throws IOException {
		config = HBaseConfiguration.create();
		executor = Executors.newFixedThreadPool(5);
		conn = ConnectionFactory.createConnection(config, executor);
		table = conn.getTable(TableName.valueOf(tableName));
	}
	
	public HashMap<byte[], byte[]> get(byte[] rowKey, byte[] colFamily, ArrayList<byte[]> columns) throws IOException {
		Get g = new Get(rowKey);
		Result result = table.get(g);
		HashMap<byte[], byte[]> retval = new HashMap<byte[], byte[]>();
		for(byte[] col: columns) {
			byte [] value = result.getValue(colFamily, col);
			retval.put(col, value);
		}
		return retval;
	}
	
	public void put(byte[] rowKey, byte[] colFamily, HashMap<byte[], byte[]> columnValues) throws IOException
	{
		Put put = new Put(rowKey);
		for(Map.Entry<byte[], byte[]> entry : columnValues.entrySet()) {
		    put.addColumn(colFamily, entry.getKey(), entry.getValue());
		}
		table.put(put);
	}

	public ArrayList<String[]> scan(byte[] columnFamily, ArrayList<byte[]> columns) throws IOException
	{
		Scan scan = new Scan();
		for(byte[] col: columns) {
		    scan.addColumn(columnFamily, col);
		}
		ArrayList<String[]> retval = new ArrayList<String[]>();
		ResultScanner scanner = table.getScanner(scan);
		for (Result result = scanner.next(); result != null; result = scanner.next())
		{
			int i = 0;
			String[] attr = new String[columns.size() + 1];
			attr[i++] = new String(result.getRow(), "UTF-8");
			for(byte[] col: columns) {
			    attr[i++] = new String(result.getValue(columnFamily, col), "UTF-8");
			}
			retval.add(attr);
		}
		scanner.close();
		return retval;
	}
	
	public void close() throws IOException {
		table.close();
	}
}
