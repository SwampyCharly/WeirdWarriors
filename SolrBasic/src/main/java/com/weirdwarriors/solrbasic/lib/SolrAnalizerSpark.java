package com.weirdwarriors.solrbasic.lib;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import com.lucidworks.spark.rdd.SolrJavaRDD;

public class SolrAnalizerSpark 
{
	private static Logger logger = Logger.getLogger(SolrAnalizerSpark.class);
	private SparkContext sc;
	private String zookeeperHost;
	private JavaSparkContext context;
	
	public SolrAnalizerSpark(String zookeeperHost)
	{
		this.zookeeperHost = zookeeperHost;
		
		SparkConf conf = new SparkConf().setAppName("SolrAnalizerSpark").setMaster("local[*]");
		context = new JavaSparkContext(conf);
		
		sc = context.sc();
		
		logger.info("Solr analizer with Spark instanced.");
	}
	
	public JavaRDD<SolrDocument> analize(String collection, String queryString, String[] queryFields, List<String> filters) throws SolrServerException, IOException
	{
		SolrJavaRDD solrRDD = SolrJavaRDD.get(zookeeperHost, collection, sc);
		
		SolrQuery query = new SolrQuery();
		query.setQuery(queryString);
		
		logger.debug("Created query " + queryString + ".");

		query.setFields(queryFields);
		logger.debug("Added fields to query.");
		
		if (filters != null)
		{
			for (String f : filters)
			{
				query.setFilterQueries(f);
			}
			
			logger.debug("Added filters to query.");
		}		
		
		JavaRDD<SolrDocument> resultsRDD = solrRDD.queryShards(query);
		long resultsNumber = resultsRDD.count();

		logger.info("Launched query to the collection " + collection + ".");
		logger.info("Found " + resultsNumber + " documents.");
		
		return resultsRDD;
	}
	
	public JavaRDD<SolrDocument> analize(String collection, String queryString, List<String> queryFields) throws SolrServerException, IOException
	{
		String[] queryFieldsArray = new String[queryFields.size()];
		queryFieldsArray = queryFields.toArray(queryFieldsArray);
		
		return analize(collection, queryString, queryFieldsArray, null);
	}
	
	public JavaRDD<SolrDocument> analize(String collection, String queryString, List<String> queryFields, List<String> filters) throws SolrServerException, IOException
	{
		String[] queryFieldsArray = new String[queryFields.size()];
		queryFieldsArray = queryFields.toArray(queryFieldsArray);
		
		return analize(collection, queryString, queryFieldsArray, filters);
	}
	
	public SolrDocumentList analizeToList(String collection, String queryString, String[] queryFields) throws SolrServerException, IOException
	{		
		return SolrUtils.toSolrDocumentsList(analize(collection, queryString, queryFields, null));
	}
	
	public SolrDocumentList analizeToList(String collection, String queryString, List<String> queryFields) throws SolrServerException, IOException
	{		
		return SolrUtils.toSolrDocumentsList(analize(collection, queryString, queryFields, null));
	}
	
	public SolrDocumentList analizeToList(String collection, String queryString, String[] queryFields, List<String> filters) throws SolrServerException, IOException
	{		
		return SolrUtils.toSolrDocumentsList(analize(collection, queryString, queryFields, filters));
	}
	
	public SolrDocumentList analizeToList(String collection, String queryString, List<String> queryFields, List<String> filters) throws SolrServerException, IOException
	{		
		return SolrUtils.toSolrDocumentsList(analize(collection, queryString, queryFields, filters));
	}
}
