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
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class PutHBase 
{	
	private String hostName = "quickstart.cloudera";
	private Connection conn;
	
	public PutHBase()
	{}
	
	public void connection() throws IOException
	{
		// HBase connection configuration
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", hostName);
        conn = ConnectionFactory.createConnection(conf);
	}
	
	public Table getTable(String namespaceString, String tableString, List<String> columnDescriptors) throws IOException
	{
		// For table name with name space
        String tableName = namespaceString + ":" + tableString;
        
        // Get table descriptor
        Admin admin = conn.getAdmin();
        HTableDescriptor tabledescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        // Add column families
        for (int i = 0; i < columnDescriptors.size(); i++)
        {
	        HColumnDescriptor familyData = new HColumnDescriptor(columnDescriptors.get(i).getBytes());
	        tabledescriptor.addFamily(familyData);
        }

        // If table does not exist, then create it
        if (!admin.tableExists(TableName.valueOf(tableName))) 
        {
            admin.createTable(tabledescriptor);
        }
        
        // Get the table
        return conn.getTable(TableName.valueOf(tableName));
	}
	
    public void put(String namespaceString, String tableString, List<String> columnDescriptors, List<PutValue> putValues) throws Exception, IOException 
    {    	
    	connection();
    	
        Table table = getTable(namespaceString, tableString, columnDescriptors);
        
        // List with puts
        List<Put> putList = new ArrayList<Put>();
        PutValue pv;
        // List with row keys, column families, columns and values
        List<ColumnValue> columnValues;
        
        String columnFamilyString;
        
        for (int i = 0; i < putValues.size(); i++)
        {
        	// Get every put value from the input list
        	pv = putValues.get(i);
        	
        	// Create instance of put
        	Put put = new Put(String.valueOf(pv.getRowKey()).getBytes());
        	
        	// Get column values for this put
        	columnValues = pv.getColumnValues();
        	
        	// Add column to the put and add put to the put list
        	for (int j = 0; j < columnValues.size(); j++)
        	{
        		columnFamilyString = columnValues.get(j).getColumnFamily();
        		if (!columnDescriptors.contains(columnFamilyString))
        		{
        			// TOREVIEW: Is this necessary?
        			throw new Exception("Incorrect column family in the put operation.");
        		}
        		
        		put.addColumn(columnFamilyString.getBytes(), columnValues.get(j).getColumn().getBytes(), columnValues.get(j).getValue().getBytes());
        		
        		putList.add(put);
        	}
        }
        
        // Put operation
        table.put(putList);
    }
    
    public void put(String namespaceString, String tableString, List<String> columnDescriptors, PutValue putValue) throws Exception, IOException 
    {    	
    	connection();
    	
        Table table = getTable(namespaceString, tableString, columnDescriptors);
        
        // List with puts
        List<Put> putList = new ArrayList<Put>();
        // List with row keys, column families, columns and values
        List<ColumnValue> columnValues;
        	
        // Create instance of put
    	Put put = new Put(String.valueOf(putValue.getRowKey()).getBytes());
    	
    	// Get column values for this put
    	columnValues = putValue.getColumnValues();
        
        String columnFamilyString;
    	
    	// Add column to the put and add put to the put list
    	for (int j = 0; j < columnValues.size(); j++)
    	{
    		columnFamilyString = columnValues.get(j).getColumnFamily();
    		if (!columnDescriptors.contains(columnFamilyString))
    		{
    			// TOREVIEW: Is this necessary?
    			throw new Exception("Incorrect column family in the put operation.");
    		}
    		
    		put.addColumn(columnValues.get(j).getColumnFamily().getBytes(), columnValues.get(j).getColumn().getBytes(), columnValues.get(j).getValue().getBytes());
    		
    		putList.add(put);
    	}
        
    	// Put operation
        table.put(putList);
    }
}
