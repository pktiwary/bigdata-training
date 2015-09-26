package demo.hbase.HBaseLib;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class App 
{
    public static void main( String[] args ) throws IOException
    {
        HBaseTable ht = new HBaseTable("people");
        
        HashMap<byte[], byte[]> newCol = new HashMap<byte[], byte[]>();
        newCol.put("first".getBytes(), "Abhineet".getBytes());
        newCol.put("last".getBytes(), "Munshi".getBytes());
        ht.put("8".getBytes(), "cf".getBytes(), newCol);
        
        System.out.println("Testing values based on rowkeys");
        ArrayList<byte[]> rowKeys = new ArrayList<byte[]>();
        rowKeys.add("1".getBytes());
        rowKeys.add("2".getBytes());
        rowKeys.add("3".getBytes());
        rowKeys.add("4".getBytes());
        rowKeys.add("5".getBytes());
        rowKeys.add("6".getBytes());
        rowKeys.add("7".getBytes());
        rowKeys.add("8".getBytes());
        
        ArrayList<byte[]> columns = new ArrayList<byte[]>();
        columns.add("first".getBytes());
        columns.add("last".getBytes());
        
        for(byte[] rowKey: rowKeys) {
	        HashMap<byte[], byte[]> values = ht.get(rowKey, "cf".getBytes(), columns);
	        for(Map.Entry<byte[], byte[]> entry : values.entrySet()) {
	        	System.out.println(new String(rowKey, "UTF-8") + " =>" +
	        			new String(entry.getKey(), "UTF-8") + " : " +
	        			new String(entry.getValue(), "UTF-8"));
	        }
        }
        
        System.out.println("Testing scan of table");
        ArrayList<String[]> rows = ht.scan("cf".getBytes(), columns);
        for(String[] row: rows) {
        	for(String col: row) {
        	    System.out.print(col + ",");
        	}
        	System.out.println("");
        }
        ht.close();
    }
}
