package com.weirdwarriors.hbasebasic.apps;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

import com.weirdwarriors.hbasebasic.lib.*;

public class HBaseApp3 
{
	private static String zookeeperPort = "2181";
	
    public static void main( String[] args )
    {
    	 try 
         {
	        PutHBase configurationhbase = new PutHBase(zookeeperPort);
	        GetHBase gethbase = new GetHBase(zookeeperPort);
	        
	        String namespaceString = "ns1";
	        String tableString = "comics";
	        
	        List<String> columnDescriptors = new ArrayList<String>();
	        columnDescriptors.add("c");
	        columnDescriptors.add("a");
	        
	        List<PutValue> putValues = new ArrayList<PutValue>();
	        
	        List<ColumnValue> columnValuesOutput = new ArrayList<ColumnValue>();
	        
	        List<ColumnValue> columnValuesSimple = new ArrayList<ColumnValue>();
	        ColumnValue cv = new ColumnValue("c", "title");
	        columnValuesSimple.add(cv);
	        cv = new ColumnValue("c", "genre");
	        columnValuesSimple.add(cv);
	        cv = new ColumnValue("c", "mainCharacter");
	        columnValuesSimple.add(cv);
	        cv = new ColumnValue("c", "mainAntagonist");
	        columnValuesSimple.add(cv);
	        cv = new ColumnValue("a", "writer");
	        columnValuesSimple.add(cv);
	        cv = new ColumnValue("a", "artist");
	        columnValuesSimple.add(cv);
	        
	        List<ColumnValue> columnValues = new ArrayList<ColumnValue>();	        
	        cv = new ColumnValue("c", "title", "Sandman");
	        columnValues.add(cv);
	        cv = new ColumnValue("c", "genre", "mystic");
	        columnValues.add(cv);
	        cv = new ColumnValue("c", "mainCharacter", "Morpheus");
	        columnValues.add(cv);
	        cv = new ColumnValue("c", "mainAntagonist", "The Furies");
	        columnValues.add(cv);
	        cv = new ColumnValue("a", "writer", "Neil Gaiman");
	        columnValues.add(cv);
	        cv = new ColumnValue("a", "artist", "Sam Kieth / Dave McKean / Craig Russell");
	        columnValues.add(cv);
	        
	        PutValue pv = new PutValue("Sandman-1978", columnValues);        
	        putValues.add(pv);
	
	        columnValues = new ArrayList<ColumnValue>();
	        cv = new ColumnValue("c", "title", "Watchmen");
	        columnValues.add(cv);
	        cv = new ColumnValue("c", "genre", "crazy superheroes");
	        columnValues.add(cv);
	        cv = new ColumnValue("c", "mainCharacter", "Rorscharch");
	        columnValues.add(cv);
	        cv = new ColumnValue("c", "mainAntagonist", "Ozymandias");
	        columnValues.add(cv);
	        cv = new ColumnValue("a", "writer", "Alan Moore");
	        columnValues.add(cv);
	        cv = new ColumnValue("a", "artist", "Dave Gibbons");
	        columnValues.add(cv);
	        
	        pv = new PutValue("Watchmen-1985", columnValues);
	        putValues.add(pv);
	        
	        columnValues = new ArrayList<ColumnValue>();
	        cv = new ColumnValue("c", "title", "Preacher");
	        columnValues.add(cv);
	        cv = new ColumnValue("c", "genre", "preachers and vampires");
	        columnValues.add(cv);
	        cv = new ColumnValue("c", "mainCharacter", "Jesse Custer");
	        columnValues.add(cv);
	        cv = new ColumnValue("c", "mainAntagonist", "Starr and Jesus Christ");
	        columnValues.add(cv);
	        cv = new ColumnValue("a", "writer", "Garth Ennis");
	        columnValues.add(cv);
	        cv = new ColumnValue("a", "artist", "Steve Dillon");
	        columnValues.add(cv);
	        
	        pv = new PutValue("Preacher-1995", columnValues);
	        putValues.add(pv);      
	        
	        columnValues = new ArrayList<ColumnValue>();
	        cv = new ColumnValue("c", "title", "Y, the Last Man");
	        columnValues.add(cv);
	        cv = new ColumnValue("c", "genre", "apocaliptic");
	        columnValues.add(cv);
	        cv = new ColumnValue("c", "mainCharacter", "Yorick Brown");
	        columnValues.add(cv);
	        cv = new ColumnValue("c", "mainAntagonist", "Japanese Doctor");
	        columnValues.add(cv);
	        cv = new ColumnValue("a", "writer", "Brian Vaughan");
	        columnValues.add(cv);
	        cv = new ColumnValue("a", "artist", "Pia Guerra");
	        columnValues.add(cv);
	        
	        pv = new PutValue("Y, the Last Man-2003", columnValues);
	        putValues.add(pv);    
	        
	        columnValues = new ArrayList<ColumnValue>();
	        cv = new ColumnValue("c", "title", "Saga");
	        columnValues.add(cv);
	        cv = new ColumnValue("c", "genre", "space opera");
	        columnValues.add(cv);
	        cv = new ColumnValue("c", "mainCharacter", "Marko, Hazel and Alana");
	        columnValues.add(cv);
	        cv = new ColumnValue("c", "mainAntagonist", "Galactic Empires of Terrada and Guirnalda");
	        columnValues.add(cv);
	        cv = new ColumnValue("a", "writer", "Brian Vaughan");
	        columnValues.add(cv);
	        cv = new ColumnValue("a", "artist", "Fiona Staples");
	        columnValues.add(cv);
	        
	        pv = new PutValue("Saga-2006", columnValues);
	        putValues.add(pv);
	        
	        columnValues = new ArrayList<ColumnValue>();
	        cv = new ColumnValue("c", "title", "100 Bullets");
	        columnValues.add(cv);
	        cv = new ColumnValue("c", "genre", "crime");
	        columnValues.add(cv);
	        cv = new ColumnValue("c", "mainCharacter", "Agent Graves and his Minutemen");
	        columnValues.add(cv);
	        cv = new ColumnValue("c", "mainAntagonist", "Crime families");
	        columnValues.add(cv);
	        cv = new ColumnValue("a", "writer", "Brian Azzarello");
	        columnValues.add(cv);
	        cv = new ColumnValue("a", "artist", "Eduardo Risso");
	        columnValues.add(cv);
	        
	        pv = new PutValue("100 Bullets-2008", columnValues);
	        putValues.add(pv);
	        columnValues = new ArrayList<ColumnValue>();
	        cv = new ColumnValue("c", "title", "Saga of the Swamp Thing");
	        columnValues.add(cv);
	        cv = new ColumnValue("c", "genre", "swamp elemental Gods and terror");
	        columnValues.add(cv);
	        cv = new ColumnValue("c", "mainCharacter", "The Swamp Thing");
	        columnValues.add(cv);
	        cv = new ColumnValue("c", "mainAntagonist", "Nukeface");
	        columnValues.add(cv);
	        cv = new ColumnValue("a", "writer", "Alan Moore");
	        columnValues.add(cv);
	        cv = new ColumnValue("a", "artist", "Bernie Wrightson");
	        columnValues.add(cv);
	        
	        pv = new PutValue("Saga of the Swamp Thing-1983", columnValues);
	        putValues.add(pv);
       
			configurationhbase.put(namespaceString, tableString, columnDescriptors, putValues);

			columnValuesOutput = gethbase.getValue(namespaceString, tableString, columnValuesSimple, "Sandman-1978");

			for (int i = 0; i < columnValuesOutput.size(); i++)
			{
				System.out.println(columnValuesOutput.get(i).toString());
			}

			gethbase.scan(namespaceString, tableString, columnValuesSimple);
			
			List<ColumnValue> columnValues1 = new ArrayList<ColumnValue>();
	        ColumnValue cv1 = new ColumnValue("c", "title", null);
	        columnValues1.add(cv1);
			
			FilterList fl = new FilterList();
			Filter fJ = new SingleColumnValueFilter("a".getBytes(), "writer".getBytes(), CompareOp.EQUAL, "Alan Moore".getBytes());
			
			fl.addFilter(fJ);

			gethbase.scan(namespaceString, tableString, columnValuesSimple, fl, null, null);
		} catch (Exception e) 
        {
			e.printStackTrace();
		}
    }
    
}
