package com.weirdwarriors.hbaseutils.apps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.weirdwarriors.hbaseutils.lib.*;

public class HBaseApp2 
{
	private static String zookeeperPort = "2181";
	private static String zookeeperNodeHBase = "/hbase";
	
	private static String namespaceString1 = "ns1";
	private static String namespaceString2 = "ns2";
	private static String namespaceString3 = "ns3";
	private static String tableString1 = "Marvel";
	private static String tableString2 = "DC";
	private static String tableString3 = "Vertigo";
	private static String tableString4 = "Image";
	private static String tableString5 = "Wildstorm";
	private static String tableString6 = "AmericanBestComics";
	private static String tableString7 = "DarkHorse";
	private static String tableString8 = "TopCow";
	private static String tableString9 = "Fantagraphics";
	private static String cf1 = "a";
	private static String cf2 = "w";
	private static String cf3 = "r";
	private static String cf4 = "p";
    
    public static void main( String[] args )
    {
    	try 
    	{
    		HBaseManager hbm = new HBaseManager(zookeeperPort, zookeeperNodeHBase);
    		
    		hbm.createNamespace(namespaceString1);
    		hbm.createNamespace(namespaceString2);
    		hbm.createNamespace(namespaceString3);
    		
    		List<String> cfList = new ArrayList<String>();
    		cfList.add(cf1);
    		cfList.add(cf2);
    		cfList.add(cf3);
    		cfList.add(cf4);
    		
    		hbm.createTable(namespaceString1, tableString1, cfList);
    		hbm.createTable(namespaceString1, tableString2, cfList);
    		hbm.createTable(namespaceString1, tableString3, cfList);
    		hbm.createTable(namespaceString2, tableString4, cfList);
    		hbm.createTable(namespaceString2, tableString5, cfList);
    		hbm.createTable(namespaceString2, tableString6, cfList);
    		hbm.createTable(namespaceString2, tableString7, cfList);
    		hbm.createTable(namespaceString3, tableString8, cfList);
    		hbm.createTable(namespaceString3, tableString9, cfList);
    		
    		hbm.deleteTable(namespaceString1, tableString1);
    		hbm.deleteTable(namespaceString1, tableString2);
    		hbm.deleteTable(namespaceString1, tableString3);
    		
    		hbm.emptyNamespace(namespaceString2);
    		
    		hbm.truncateTable(namespaceString3, tableString8);
    		hbm.truncateTable(namespaceString3, tableString9);
    		
    		hbm.deleteNamespace(namespaceString1);
    		hbm.deleteNamespace(namespaceString2);
    		hbm.deleteNamespace(namespaceString3);
		} catch (IOException e) 
    	{
			e.printStackTrace();
		}
    }
    
}
