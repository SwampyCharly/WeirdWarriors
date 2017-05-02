package com.weirdwarriors.hbaseutils.hbaseutils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

public class GetHBase 
{	
	private String hostName = "quickstart.cloudera";
	private Connection conn;
	
	public GetHBase()
	{}
	
	public void connection() throws IOException
	{
		// HBase connection configuration
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", hostName);
        conn = ConnectionFactory.createConnection(conf);
	}
	
	public Table getTable(String namespaceString, String tableString) throws IOException
	{
		// For table name with name space
        String tableName = namespaceString + ":" + tableString;
        
        // Get the table
        return conn.getTable(TableName.valueOf(tableName));
	}
	
    public List<ColumnValue> getValue(String namespaceString, String tableString, List<ColumnValue> listColumns, String rowKey) throws IOException 
    {    	
    	connection();

    	Table table = getTable(namespaceString, tableString);
        
        List<ColumnValue> listValues = new ArrayList<ColumnValue>();
        
        // Create instance of get using row key
        Get get = new Get(rowKey.getBytes());
        
        // Store result
        Result result = table.get(get);
        
        // Get only the input column values
        for (int i = 0; i < listColumns.size(); i++)
        {
        	byte[] columnFamily = listColumns.get(i).getColumnFamily().getBytes();
        	byte[] column = listColumns.get(i).getColumn().getBytes();
        	// Get value 
        	byte[] value =  result.getValue(columnFamily, column);
        	
        	// TOREVIEW: Is this necessary? Test if this column does not exist
        	if (value != null)
        	{
        		// Form the list with the column values
	        	ColumnValue cv = new ColumnValue(rowKey, listColumns.get(i).getColumnFamily(), listColumns.get(i).getColumn(), Bytes.toString(value));
	        	
	        	cv.print();
	        	
	        	listValues.add(cv);
        	}
        }
        
        return listValues;
    }

    public List<ColumnValue> scan(String namespaceString, String tableString, List<ColumnValue> listColumns) throws Exception, IOException 
    {
    	return scan(namespaceString, tableString, listColumns);       
	}
    
    public List<ColumnValue> scan(String namespaceString, String tableString, List<ColumnValue> listColumns, String startRow) throws Exception, IOException 
    {
    	return scan(namespaceString, tableString, listColumns, startRow);       
	}
    
    public List<ColumnValue> scan(String namespaceString, String tableString, List<ColumnValue> listColumns, String startRow, String endRow) throws Exception, IOException 
    {
    	return scan(namespaceString, tableString, listColumns, startRow, endRow);       
	}
    
    public List<ColumnValue> scan(String namespaceString, String tableString, List<ColumnValue> listColumns, FilterList filterList) throws Exception, IOException 
    {
    	return scan(namespaceString, tableString, listColumns, filterList);       
	}
    
    public List<ColumnValue> scan(String namespaceString, String tableString, List<ColumnValue> listColumns, FilterList filterList, String startRow) throws Exception, IOException 
    {
    	return scan(namespaceString, tableString, listColumns, filterList, startRow);       
	}
    
    public List<ColumnValue> scan(String namespaceString, String tableString, List<ColumnValue> listColumns, FilterList filterList, String startRow, String endRow) throws Exception, IOException 
    {
    	connection();

    	Table table = getTable(namespaceString, tableString);
        
        Scan scan = null;
        byte[] startRowBytes;
        byte[] endRowBytes;
        
        if (startRow != null)
        {
        	startRowBytes = startRow.getBytes();
        	
        	if (endRow != null)
            {
        		endRowBytes = endRow.getBytes();
        		// Scan with start row and end row
            	scan = new Scan(startRowBytes, endRowBytes);
            }
        	else
        	{
        		// Scan with start row
        		scan = new Scan(startRowBytes);
        	}
        }
        else
        {
        	if (endRow != null)
            {
        		// No scan if there is end row without start row
            	throw new Exception("Scan constructor must have start row parameter if there is end row parameter.");
            }
        	else
        	{
        		// Scan without start row and end row
        		scan = new Scan();
        	}
        }
        
        // Set filter list if exists
        if (filterList != null)
        {
        	scan.setFilter(filterList);
        }
        
        // Column values output list
        List<ColumnValue> listColumnsOutput = new ArrayList<ColumnValue>();        
        String rowKeyString;
        
        // Scan operation 
        ResultScanner scanner = table.getScanner(scan);
        
        for (Result scannerResult : scanner) 
        {    	    
        	// Get row key string
            byte[] bytesResult = scannerResult.getRow();            
            rowKeyString = Bytes.toString(bytesResult);
            
            // Get only the input column values
            for (int i = 0; i < listColumns.size(); i++)
            {
            	// Get value
            	byte[] bytesValue = scannerResult.getValue(listColumns.get(i).getColumnFamily().getBytes(), listColumns.get(i).getColumn().getBytes());
            	
            	// TOREVIEW: Is this necessary? Test if this column does not exist
            	if (bytesValue != null)
            	{
	            	// Form the list with the column values
		        	ColumnValue cv = new ColumnValue(rowKeyString, listColumns.get(i).getColumnFamily(), listColumns.get(i).getColumn(), Bytes.toString(bytesValue));
	            	
	            	cv.print();
	                
	            	listColumnsOutput.add(cv);  
            	}
            }      	      
        }
        
        return listColumnsOutput;           
    }
}
        
        
        
 

    


