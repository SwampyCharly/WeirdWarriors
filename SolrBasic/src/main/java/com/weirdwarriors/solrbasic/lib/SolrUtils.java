package com.weirdwarriors.solrbasic.lib;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.JavaRDD;

public class SolrUtils 
{
	private static Logger logger = Logger.getLogger(SolrUtils.class);
	
	public static SolrInputDocument toSolrInputDocument(SolrDocument d)
	{
	  SolrInputDocument doc = new SolrInputDocument();
	  for( String name : d.getFieldNames() ) 
	  {
	    doc.addField( name, d.getFieldValue(name), 1.0f );
	  }
	  
	  return doc;
	}
	
	public static List<SolrInputDocument> toSolrInputDocumentsList(JavaRDD<SolrDocument> docsRDD)
	{		
	  List<SolrDocument> docs = toSolrDocumentsList(docsRDD);
	  
	  List<SolrInputDocument> inputDocs = new ArrayList<SolrInputDocument>();
	  
	  for (SolrDocument doc : docs)
	  {
		  inputDocs.add(SolrUtils.toSolrInputDocument(doc));
	  }
	  
	  return inputDocs;
	}
	
	public static SolrDocumentList toSolrDocumentsList(JavaRDD<SolrDocument> docsRDD)
	{
	  List<SolrDocument> list = docsRDD.collect();
	  SolrDocumentList docList = new SolrDocumentList();
	  docList.addAll(list);
	  
	  return docList;
	}
	
	public static void print(SolrDocument d)
	{
		Collection<String> fieldNames = d.getFieldNames();
		
		for (String s : fieldNames)
		{
			System.out.println("FIELD: " + s + " | VALUE: " + d.getFieldValue(s));
		}
	}
	
	public static void print(SolrInputDocument d)
	{
		Collection<String> fieldNames = d.getFieldNames();
		
		for (String s : fieldNames)
		{
			System.out.println("FIELD: " + s + " | VALUE: " + d.getFieldValue(s));
		}
	}
	
	public static boolean compareFieldNames(SolrDocument d1, SolrDocument d2)
	{
		Collection<String> fieldNames1 = d1.getFieldNames();
		Collection<String> fieldNames2 = d2.getFieldNames();
		
		if (fieldNames1.equals(fieldNames2))
		{
			return true;
		}
		
		return false;
	}
	
	public static boolean compareFieldNames(SolrInputDocument d1, SolrInputDocument d2)
	{
		Collection<String> fieldNames1 = d1.getFieldNames();
		Collection<String> fieldNames2 = d2.getFieldNames();
		
		if (fieldNames1.equals(fieldNames2))
		{
			return true;
		}
		
		return false;
	}
	
	public static boolean compare(SolrInputDocument d1, SolrInputDocument d2)
	{
		Collection<String> fieldNames1 = d1.getFieldNames();
		Collection<String> fieldNames2 = d2.getFieldNames();
		
		if (fieldNames1.equals(fieldNames2))
		{
			for (String fieldName : fieldNames1)
			{
				logger.debug("FIELD " + fieldName);
				logger.debug("VALUE1 "+ d1.getFieldValue(fieldName));
				logger.debug("VALUE2 "+ d2.getFieldValue(fieldName));
				if (!d1.getFieldValue(fieldName).equals(d2.getFieldValue(fieldName)))
				{
					return false;
				}
			}
		}
		else
		{
			return false;
		}
		
		return true;
	}
}
