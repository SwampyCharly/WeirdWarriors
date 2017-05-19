package com.weirdwarriors.hbasebasic.apps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.weirdwarriors.hbasebasic.lib.*;

public class HBaseApp4 
{
	private static String zookeeperPort = "2181";
	private static String zookeeperNodeHBase = "/hbase";
	
	private static String namespaceString = "ns10";
	private static String usersTableString = "users";
	private static String authorsTableString = "authors";
	private static String eventsTableString = "events";
	private static String comicsTableString = "comics";
	private static String HDFSPath = "/user/cloudera/cr/";
	private static String columnFamilyString = "f";	
	private static String separator = "\t";
    
    public static void main( String[] args )
    {
    	try 
    	{   
    		HBaseManager hbm = new HBaseManager(zookeeperPort, zookeeperNodeHBase);
    		
    		hbm.deleteNamespace(namespaceString);
    		
    		HBaseImport hbi = new HBaseImport(zookeeperPort, zookeeperNodeHBase);
    		
    		List<String> userFields = new ArrayList<String>();
    		userFields.add("id");
    		userFields.add("name");
    		userFields.add("surname");
    		userFields.add("age");
    		userFields.add("gender");
    		userFields.add("country");
    		userFields.add("registerDate");
        
			hbi.importFromHDFS(namespaceString, usersTableString, columnFamilyString, HDFSPath + usersTableString, userFields, separator);
			
			List<String> authorFields = new ArrayList<String>();
			authorFields.add("id");
    		authorFields.add("name");
    		authorFields.add("surname");
    		authorFields.add("age");
    		authorFields.add("gender");
    		authorFields.add("country");
        
			hbi.importFromHDFS(namespaceString, authorsTableString, columnFamilyString, HDFSPath + authorsTableString, authorFields, separator);
			
			List<String> comicFields = new ArrayList<String>();
			comicFields.add("id");
			comicFields.add("writerId");
			comicFields.add("artistId");
			comicFields.add("title");
			comicFields.add("volume");
			comicFields.add("issue");
			comicFields.add("publisher");
			comicFields.add("publicationDate");
        
			hbi.importFromHDFS(namespaceString, comicsTableString, columnFamilyString, HDFSPath + comicsTableString, comicFields, separator);
			
			List<String> eventFields = new ArrayList<String>();
			eventFields.add("userId");
			eventFields.add("comicId");
    		eventFields.add("eventDate");
    		
    		int[] rowKeyFields = new int[3];
    		rowKeyFields[0] = 0;
    		rowKeyFields[1] = 1;
    		rowKeyFields[2] = 2;

    		hbi.setRowKeyFields(rowKeyFields);
			hbi.importFromHDFS(namespaceString, eventsTableString, columnFamilyString, HDFSPath + eventsTableString, eventFields, separator);
		} catch (IOException e) 
    	{
			e.printStackTrace();
		}
    }
    
}
