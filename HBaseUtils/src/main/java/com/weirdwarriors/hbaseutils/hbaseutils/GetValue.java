package com.weirdwarriors.hbaseutils.hbaseutils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class GetValue {
	
	private final static String hostName = "quickstart.cloudera";
	
	public GetValue()
	{}
	
    public  List<ColumnValue>  getValue(String namespaceString, String tableString, List<ColumnValue> listColumns, String rowKey) throws IOException {


    	
        // Configura la conexión a HBase
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", hostName);
        Connection conn = ConnectionFactory.createConnection(conf);

        String tableName = namespaceString + ":" + tableString;


        
        // Obten la tabla
        Table table = conn.getTable(TableName.valueOf(tableName));
        
        List<ColumnValue> listValues = new ArrayList<ColumnValue>();
        Get get = new Get(rowKey.getBytes());
        Result result = table.get(get);
        for (int i = 0; i < listColumns.size(); i++){
        	byte [] columnFamily = listColumns.get(i).getColumnFamily().getBytes();
        	byte [] column = listColumns.get(i).getColumn().getBytes();
        	byte  [] value =  result.getValue(columnFamily, column);
//        	System.out.println(Bytes.toString(value));
        	ColumnValue cv = new ColumnValue(listColumns.get(i).getColumnFamily(),listColumns.get(i).getColumn(),Bytes.toString(value));
        	listValues.add(cv);
        }
        return listValues;

    }


public  List<ColumnValue>  getAllValues(String namespaceString, String tableString, List<ColumnValue> listColumns) throws IOException {


    // Configura la conexión a HBase
    org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    conf.set("hbase.zookeeper.quorum", hostName);
    Connection conn = ConnectionFactory.createConnection(conf);

    String tableName = namespaceString + ":" + tableString;


    
    // Obten la tabla
    Table table = conn.getTable(TableName.valueOf(tableName));
    
    //the next code gives all the records from the table
    Scan scan = new Scan();
    int i = 0;
    List<ColumnValue> listValues = new ArrayList<ColumnValue>();
    ResultScanner scanner = table.getScanner(scan);
    for (Result scannerResult : scanner){
    	byte [] columnFamily = listColumns.get(i).getColumnFamily().getBytes();
    	byte [] column = listColumns.get(i).getColumn().getBytes();
        byte  [] value =  scannerResult.getValue(columnFamily, column);
//        	        	System.out.println(Bytes.toString(value));
        i++;
        ColumnValue cv = new ColumnValue(listColumns.get(i).getColumnFamily(),listColumns.get(i).getColumn(),Bytes.toString(value));
    	listValues.add(cv);
        	      
        }
    return listValues;
       
	}
}
        
        
        
 

    


