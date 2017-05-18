package com.weirdwarriors.hbaseutils.lib;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.Logger;

public class HBaseManager 
{
	private static Logger logger = Logger.getLogger(HBaseManager.class);
	private static int DELETE_MODE = 0;
	private static int TRUNCATE_MODE = 1;
	private Configuration HBaseConfig;
	private Admin admin;
	private String hostName;
	
	public HBaseManager(String zookeeperPort, String zookeeperNodeHBase) throws IOException
	{
		hostName = InetAddress.getLocalHost().getHostName();
		
		HBaseConfig = HBaseConfiguration.create();
		HBaseConfig.set("hbase.zookeeper.quorum", hostName);
		HBaseConfig.set("hbase.zookeeper.property.clientPort", zookeeperPort);
		HBaseConfig.set("zookeeper.znode.parent", zookeeperNodeHBase);	
		
		logger.info("HBase configuration created and HBaseManager instanced.");
		
		Connection HBaseConnection = ConnectionFactory.createConnection(HBaseConfig);
		admin = HBaseConnection.getAdmin();
		
		logger.info("Connection to HBase accepted.");
	}
	
	public boolean isNamespaceCreated(String namespaceString) throws IOException
	{
		NamespaceDescriptor[] descriptors= admin.listNamespaceDescriptors();	
		
		for (int i = 0; i < descriptors.length; i++)
		{
			if (descriptors[i].getName().equals(namespaceString))
			{			
				logger.info("Namespace " + namespaceString + " already exists.");
				
				return true;
			}
		}
		
		logger.info("Namespace " + namespaceString + " does not exist.");
		
		return false;
	}
	
	public boolean isTableCreated(String namespaceString, String tableString) throws IOException
	{
		if (!isNamespaceCreated(namespaceString))
		{
			logger.info("Namespace " + namespaceString + " does not exist.");
			
			return false;
		}
		
		TableName tableName = TableName.valueOf(namespaceString + ":" + tableString);
		
		if (!admin.tableExists(tableName)) 
		{
			logger.info("Table " + tableName + " does not exist.");
			
			return false;
		}
		else
		{
			logger.info("Table " + tableName + " already exists.");
			
			return true;
		}
	}
	
	public void createNamespace(String namespaceString) throws IOException
	{		
		if (!isNamespaceCreated(namespaceString))
		{
			NamespaceDescriptor nsDescriptor = NamespaceDescriptor.create(namespaceString).build();
			admin.createNamespace(nsDescriptor);
			
			logger.info("Namespace " + namespaceString + " created.");	
		}
	}
	
	public void createTable(String namespaceString, String tableString) throws IOException
	{		
		createTable(namespaceString, tableString, null);
	}
	
	public void createTable(String namespaceString, String tableString, List<String> cfList) throws IOException
	{		
		createNamespace(namespaceString);
		
		TableName tableName = TableName.valueOf(namespaceString + ":" + tableString);
		
		if (!isTableCreated(namespaceString, tableString)) 
		{
			HTableDescriptor tabledescriptor = new HTableDescriptor(tableName);
			
			if (cfList != null)
			{
				for (String cf : cfList)
				{
					HColumnDescriptor columnFamilyDescriptor = new HColumnDescriptor(cf.getBytes());
					tabledescriptor.addFamily(columnFamilyDescriptor);
					columnFamilyDescriptor.setMaxVersions(1);
				}
			}
			
			admin.createTable(tabledescriptor);
			
			logger.info("Table " + tableName + " created.");
		}
		else
		{
			logger.info("Table " + tableName + " already exists.");
		}
	}
	
	public boolean emptyNamespace(String namespaceString) throws IOException
	{		
		if (isNamespaceCreated(namespaceString))
		{				
				logger.info("Namespace " + namespaceString + " exists.");
				
				TableName[] tables = admin.listTableNamesByNamespace(namespaceString);				
				for (int j = 0; j < tables.length; j++)
				{
					admin.disableTable(tables[j]);
					admin.deleteTable(tables[j]);
					logger.info("Table " + tables[j] + " deleted.");
				}
				
				logger.info("Namespace " + namespaceString + " empty.");
				return true;			
		}
		
		logger.info("Namespace " + namespaceString + " does not exist.");	
		return false;
	}
	
	public void deleteNamespace(String namespaceString) throws IOException
	{		
		if (emptyNamespace(namespaceString))
		{
			admin.deleteNamespace(namespaceString);
			
			logger.info("Namespace " + namespaceString + " deleted.");
		}
	}
	
	private void deleteTableMode(String namespaceString, String tableString, int mode) throws IOException
	{	
		String tableName = namespaceString + ":" + tableString;
		TableName tName = TableName.valueOf(tableName);
		
		if (isTableCreated(namespaceString, tableString))
		{
			if (mode == DELETE_MODE)
			{
				admin.disableTable(tName);
				admin.deleteTable(tName);
				
				logger.info("Table " + tableName + " deleted.");
			}
			else if (mode == TRUNCATE_MODE)
			{
				admin.disableTable(tName);
				admin.truncateTable(tName, false);
				
				logger.info("Table " + tableName + " truncated.");
			}
			else
			{
				logger.info("Mode not supported.");
			}
		}
	}
	
	public void truncateTable(String namespaceString, String tableString) throws IOException
	{	
		deleteTableMode(namespaceString, tableString, TRUNCATE_MODE);
	}
	
	public void deleteTable(String namespaceString, String tableString) throws IOException
	{	
		deleteTableMode(namespaceString, tableString, DELETE_MODE);
	}
}
