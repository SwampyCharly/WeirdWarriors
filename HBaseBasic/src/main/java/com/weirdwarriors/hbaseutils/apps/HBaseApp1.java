package com.weirdwarriors.hbaseutils.apps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.weirdwarriors.hbaseutils.lib.*;

public class HBaseApp1 
{
	private static String zookeeperPort = "2181";
	private static String zookeeperNodeHBase = "/hbase";
	
	private static String namespaceString = "ns1";
	private static String tableString = "supes";
	private static String HDFSPath = "/user/cloudera/superheroes/";
	private static String columnFamilyString = "f";	
	private static String separator = ",";
    
    public static void main( String[] args )
    {
    	try 
    	{   
    		HBaseManager hbm = new HBaseManager(zookeeperPort, zookeeperNodeHBase);
    		
    		hbm.deleteNamespace(namespaceString);
    		
    		HBaseImport it = new HBaseImport(zookeeperPort, zookeeperNodeHBase);
    		
    		List<String> fields = new ArrayList<String>();
    		fields.add("id");
    		fields.add("name");
    		fields.add("age");
    		fields.add("power");
        
			it.importFromHDFS(namespaceString, tableString, columnFamilyString, HDFSPath, fields, separator);
		} catch (IOException e) 
    	{
			e.printStackTrace();
		}
    }
    
}
