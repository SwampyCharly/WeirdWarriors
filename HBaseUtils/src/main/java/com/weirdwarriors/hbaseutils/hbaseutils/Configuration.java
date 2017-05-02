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

public class Configuration {
	
    public void configuration(String namespaceString, String tableString, String[] columnDescriptors) throws IOException {

    	JavaSparkContext jsc = new JavaSparkContext();
    	
        // Configura la conexión a HBase
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "localhost");
        Connection conn = ConnectionFactory.createConnection(conf);

        String tableName = namespaceString + ":" + tableString;
        
        // crea la tabla si no existe
        Admin admin = conn.getAdmin();
        HTableDescriptor tabledescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        for (int i = 0; i < columnDescriptors.length; i++)
        {
	        HColumnDescriptor familyData = new HColumnDescriptor(columnDescriptors[i].getBytes());
	        tabledescriptor.addFamily(familyData);
        }

        if (!admin.tableExists(TableName.valueOf(tableName))) {
            admin.createTable(tabledescriptor);
        }
        
        // Obten la tabla
        Table table = conn.getTable(TableName.valueOf(tableName));
        
        JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);
        
        List<String> list = new ArrayList<String>(5);
        JavaRDD<String> rdd = jsc.parallelize(list);

        // Inserta 2 valores
        Put put = new Put("1".getBytes());
        put.addColumn("datos".getBytes(), "nombre".getBytes(), "Juan".getBytes());
        put.addColumn("datos".getBytes(), "apellido".getBytes(), "García".getBytes());
        put.addColumn("trabajo".getBytes(), "puesto".getBytes(), "desarrollador".getBytes());
        put.addColumn("trabajo".getBytes(), "experiencia".getBytes(), "4".getBytes());

        Put put2 = new Put("2".getBytes());
        put2.addColumn("datos".getBytes(), "nombre".getBytes(), "Mario".getBytes());
        put2.addColumn("datos".getBytes(), "apellido".getBytes(), "Rossi".getBytes());
        put2.addColumn("trabajo".getBytes(), "puesto".getBytes(), "desarrollador".getBytes());
        put2.addColumn("trabajo".getBytes(), "experiencia".getBytes(), "3".getBytes());

        List<Put> list = new ArrayList<Put>();
        list.add(put);
        list.add(put2);
        table.put(list);

        // obten un valor y imprimelo
        Get get1 = new Get("1".getBytes());
        Result result1 = table.get(get1);
        byte[] apellido1 = result1.getValue("datos".getBytes(), "apellido".getBytes());
        byte[] puesto1 = result1.getValue("trabajo".getBytes(), "puesto".getBytes());

        System.out.println(Bytes.toString(apellido1) + " " + Bytes.toString(puesto1));

        // haz un scan de la tabla y imprime clave y nombre por cada registro
        Scan scan = new Scan();
        scan.addColumn("datos".getBytes(), "nombre".getBytes());

        for (Result result : table.getScanner(scan)) {
            System.out.println("Found row " + Bytes.toString(result.getRow()) + ": "
                    + Bytes.toString(result.getValue("datos".getBytes(), "nombre".getBytes())));
        }

        // borra el valor con clave 2
        Delete delete = new Delete("2".getBytes());
        table.delete(delete);
        System.out.println("Borrado valor 2");

        // repite el scan para verificar el borrado
        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner) {
            System.out.println("Found row " + Bytes.toString(result.getRow()) + ": "
                    + Bytes.toString(result.getValue("datos".getBytes(), "nombre".getBytes())));
        }
        scanner.close();

    }

}
