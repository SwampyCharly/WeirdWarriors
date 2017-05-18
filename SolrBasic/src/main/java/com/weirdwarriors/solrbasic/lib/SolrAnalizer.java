package com.weirdwarriors.solrbasic.lib;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;

public class SolrAnalizer 
{
	private static Logger logger = Logger.getLogger(SolrAnalizer.class);
	private SolrClient client;
	
	public SolrAnalizer(String zookeeperHost)
	{
		client = new CloudSolrClient(zookeeperHost);
		
		logger.info("Solr analizer instanced.");
	}
	
	public void closeConnection() throws IOException
	{
		client.close();
		
		logger.info("Solr analizer closes connection.");	
	}
	
	public QueryResponse analize(String collection, String queryString, String[] queryFields, List<String> filters) throws SolrServerException, IOException
	{
		SolrQuery query = new SolrQuery();
		query.setQuery(queryString);
		
		logger.debug("Created query " + queryString + ".");

		query.setFields(queryFields);
		if (filters != null)
		{
			for (String f : filters)
			{
				query.setFilterQueries(f);
			}
			
			logger.debug("Added filters to query.");
		}
		
		logger.debug("Added fields to query.");
		
		QueryResponse qr = client.query(collection, query);
		
		logger.info("Launched query to the collection " + collection + ".");
		
		return qr;
	}
	
	public QueryResponse analize(String collection, String queryString, List<String> queryFields, List<String> filters) throws SolrServerException, IOException
	{
		String[] queryFieldsArray = new String[queryFields.size()];
		queryFieldsArray = queryFields.toArray(queryFieldsArray);
		
		return analize(collection, queryString, queryFieldsArray, filters);
	}
	
	public QueryResponse analize(String collection, String queryString, List<String> queryFields) throws SolrServerException, IOException
	{
		String[] queryFieldsArray = new String[queryFields.size()];
		queryFieldsArray = queryFields.toArray(queryFieldsArray);
		
		return analize(collection, queryString, queryFieldsArray, null);
	}
	
	public QueryResponse analize(String collection, String queryString, String[] queryFields) throws SolrServerException, IOException
	{
		return analize(collection, queryString, queryFields, null);
	}
	
}
