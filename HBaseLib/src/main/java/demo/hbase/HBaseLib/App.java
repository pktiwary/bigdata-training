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
	        	System.out.println(new String(rowKey, "UTF-8") + " => " +
	        			new String(entry.getKey(), "UTF-8") + " : " +
	        			new String(entry.getValue(), "UTF-8"));
	        }
        }
    }
}
