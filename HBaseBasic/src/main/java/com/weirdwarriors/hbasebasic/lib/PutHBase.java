package com.weirdwarriors.hbasebasic.lib;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;

public class PutHBase 
{	
	private static Logger logger = Logger.getLogger(PutHBase.class);
	private String hostName;
	private Connection connection;
	
	public PutHBase(String zookeeperPort) throws IOException
	{
		hostName = InetAddress.getLocalHost().getHostName();
		
		// HBase connection configuration
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", zookeeperPort);
        conf.set("hbase.zookeeper.quorum", hostName);
        connection = ConnectionFactory.createConnection(conf);
        
        logger.info("HBase configuration created and PutHBase instanced.");
	}
	
	public Table getTable(String namespaceString, String tableString) throws IOException
	{
		// For table name with name space
        String tableName = namespaceString + ":" + tableString;
        
        // Get the table
        return connection.getTable(TableName.valueOf(tableName));
	}
	
    public void put(String namespaceString, String tableString, List<String> columnDescriptors, List<PutValue> putValues) throws Exception, IOException 
    {    	
    	Table table = getTable(namespaceString, tableString);
        
        // List with puts
        List<Put> putList = new ArrayList<Put>();
        PutValue pv;
        // List with row keys, column families, columns and values
        List<ColumnValue> columnValues;
        
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
        		put.addColumn(columnValues.get(j).getColumnFamily().getBytes(), columnValues.get(j).getQualifier().getBytes(), columnValues.get(j).getValue().getBytes());
        		
        		putList.add(put);
        	}
        }
        
        // Put operation
        table.put(putList);
    }
    
    public void put(String namespaceString, String tableString, List<String> columnDescriptors, PutValue putValue) throws Exception, IOException 
    {    	
    	List<PutValue> putList = new ArrayList<PutValue>();
    	putList.add(putValue);
    	
    	put(namespaceString, tableString, columnDescriptors, putList);
    }
}
