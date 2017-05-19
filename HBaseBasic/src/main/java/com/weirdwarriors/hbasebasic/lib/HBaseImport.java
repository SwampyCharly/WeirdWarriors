package com.weirdwarriors.hbasebasic.lib;

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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class HBaseImport 
{
	private static Logger logger = Logger.getLogger(HBaseImport.class);
	private JavaSparkContext context;
	private Configuration HBaseConfig;
	private String hostName;
	private static String separator;
	private static String columnFamilyString;
	private static List<String> fields;
	private JavaHBaseContext HBaseContext;
	private Admin admin;
	private static int rowKeyFieldsNumber;
	private static int[] rowKeyFields;
	
	public HBaseImport(String zookeeperPort, String zookeeperNodeHBase) throws IOException
	{
		hostName = InetAddress.getLocalHost().getHostName();
		
		HBaseConfig = HBaseConfiguration.create();
		HBaseConfig.set("hbase.zookeeper.quorum", hostName);
		HBaseConfig.set("hbase.zookeeper.property.clientPort", zookeeperPort);
		HBaseConfig.set("zookeeper.znode.parent", zookeeperNodeHBase);	
		
		logger.info("HBase configuration created and ImportToHBase instanced.");
		
		SparkConf conf = new SparkConf().setAppName("ImportToHBase").setMaster("local[*]");
		context = new JavaSparkContext(conf);
		
		HBaseContext = new JavaHBaseContext(context, HBaseConfig);
		Connection HBaseConnection = ConnectionFactory.createConnection(HBaseConfig);
		admin = HBaseConnection.getAdmin();
		
		// Init row key fields number to 0. In this case, row key will be only the first field.
		rowKeyFieldsNumber = 0;
		
		logger.info("Connection to HBase accepted.");
	}
	
	public void importFromHDFS(String namespaceString, String tableString, String columnFamilyString, String HDFSPath, List<String> fields, String separator) throws IOException
	{
		NamespaceDescriptor[] descriptors= admin.listNamespaceDescriptors();	
		
		boolean namespaceExists = false;
		for (int i = 0; i < descriptors.length; i++)
		{
			if (descriptors[i].getName().equals(namespaceString))
			{
				namespaceExists = true;
				
				logger.info("Namespace " + namespaceString + " already exists.");
			}
		}		
		
		if (!namespaceExists)
		{
			NamespaceDescriptor nsDescriptor = NamespaceDescriptor.create(namespaceString).build();
			admin.createNamespace(nsDescriptor);
			
			logger.info("Namespace " + namespaceString + " created.");
		}
		
		TableName tableName = TableName.valueOf(namespaceString + ":" + tableString);
		
		if (!admin.tableExists(tableName)) 
		{
			HTableDescriptor tabledescriptor = new HTableDescriptor(tableName);
			HColumnDescriptor columnFamilyDescriptor = new HColumnDescriptor(columnFamilyString.getBytes());
			tabledescriptor.addFamily(columnFamilyDescriptor);
			columnFamilyDescriptor.setMaxVersions(1);
			
			admin.createTable(tabledescriptor);
			
			logger.info("Table " + tableName + " created.");
		}
		else
		{
			logger.info("Table " + tableName + " already exists.");
		}
		
		String totalHDFSPath = "hdfs://" + hostName + ":8020" + HDFSPath;	
		
		setSeparator(separator);
		setFields(fields);
		setColumnFamilyString(columnFamilyString);
		
		JavaRDD<String> lines = context.textFile(totalHDFSPath);
		
		logger.info("HDFS data directory stablished in " + totalHDFSPath + ".");
		
		HBaseContext.bulkPut(lines, tableName, new PutFunction());
		
		logger.info("Put done in the table " + tableName + ".");
		
		// Init row key fields number for the next use of the function
		rowKeyFieldsNumber = 0;
	}	
	
	public static String getSeparator() {
		return separator;
	}

	public static void setSeparator(String separator) {
		HBaseImport.separator = separator;
	}

	public static List<String> getFields() {
		return fields;
	}

	public static void setFields(List<String> fields) {
		HBaseImport.fields = fields;
	}

	public static String getColumnFamilyString() {
		return columnFamilyString;
	}

	public static void setColumnFamilyString(String columnFamilyString) {
		HBaseImport.columnFamilyString = columnFamilyString;
	}
	
	public void setRowKeyFields(int[] rowKeyFields)
	{
		rowKeyFieldsNumber = rowKeyFields.length;
		HBaseImport.rowKeyFields = new int[rowKeyFieldsNumber];
		for (int i = 0; i < rowKeyFields.length; i++)
		{
			HBaseImport.rowKeyFields[i] = rowKeyFields[i];
		}
	}

	public static int getRowKeyFieldsNumber()
	{
		return rowKeyFieldsNumber;
	}
	
	public static int[] getRowKeyFields()
	{
		return rowKeyFields;
	}
	
	public static String makeRowKey(String[] fields)
	{
		String rowKey = "";
		int[] rowKeyFields = getRowKeyFields();
		int rowKeyFieldsNumber = getRowKeyFieldsNumber();
		for (int i = 0; i < rowKeyFieldsNumber; i++)
		{
			rowKey += fields[rowKeyFields[i]] + "_"; 
		}
		
		// Ignore the last separator character
		return rowKey.substring(0, rowKey.length() - 1);
	}

	public static class PutFunction implements Function<String, Put> 
	{
		private static final long serialVersionUID = 1L;

		public Put call(String line) throws Exception 
	    {
		  Put put;
	      String[] part = line.split(getSeparator());
	      int i = 1;

	      // This function assumes the first field is the id (assigned to the row key)
	      if (getRowKeyFieldsNumber() == 0)
	      {
	    	  put = new Put(Bytes.toBytes(part[0]));
	      }
	      else
	      {
	    	  put = new Put(Bytes.toBytes(makeRowKey(part)));
	    	  i = 0;
	      }
	      
	      int fieldsNumber = part.length;
	      
	      // If fields number is zero or inconsistent, then add a default field
	      if (fieldsNumber == 0)
	      {
	    	  put.addColumn(Bytes.toBytes(getColumnFamilyString()), "default".getBytes(), "".getBytes());
	    	  
	    	  return put;
	      }
	      
	      for (; i < getFields().size(); i++)
	      {
	    	  String field = getFields().get(i);
	    	  
	    	  if ((fieldsNumber > i) && (part[i] != null))
		      {
	    		  put.addColumn(Bytes.toBytes(getColumnFamilyString()), Bytes.toBytes(field), Bytes.toBytes(part[i]));
		      }
	    	  else
	    	  {
	    		  put.addColumn(Bytes.toBytes(getColumnFamilyString()), Bytes.toBytes(field), "".getBytes());
	    	  }
	      }
	      
	      return put;
	    }

	}
}
