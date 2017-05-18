package com.weirdwarriors.solrbasic.lib;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

public class SolrIndexer 
{
	private static Logger logger = Logger.getLogger(SolrIndexer.class);
	private SolrClient client;
	
	public SolrIndexer(String zookeeperHost)
	{
		client = new CloudSolrClient(zookeeperHost);
		
		logger.info("Solr indexer instanced.");	
	}
	
	public void closeConnection() throws IOException
	{
		client.close();
		
		logger.info("Solr indexer closes connection.");	
	}
	
	public void indexToCollection(String collection, SolrInputDocument doc) throws SolrServerException, IOException
	{
		client.add(collection, doc);
		
		client.commit(collection);
		
		logger.info("Added document " + doc.toString() + " to the collection " + collection + ".");
	}
	
	public void indexToCollection(String collection, SolrDocument doc) throws SolrServerException, IOException
	{
		client.add(collection, SolrUtils.toSolrInputDocument(doc));
		
		client.commit(collection);
		
		logger.info("Added document " + doc.toString() + " to the collection " + collection + ".");
	}
	
	public void indexToCollection(String collection, SolrDocumentList docs) throws SolrServerException, IOException
	{
		for (SolrDocument doc : docs)
		{	
			client.add(collection, SolrUtils.toSolrInputDocument(doc));
			
			logger.info("Added document " + doc.toString() + " to the collection " + collection + ".");			
		}
		
		client.commit(collection);
		
		logger.info("Added document list to the collection " + collection + " successfully.");		
	}
	
	public void indexToCollection(String collection, QueryResponse qr) throws SolrServerException, IOException
	{
		SolrDocumentList docs = qr.getResults();	
		for (SolrDocument doc : docs)
		{			
			client.add(collection, SolrUtils.toSolrInputDocument(doc));
			
			logger.info("Added document " + doc.toString() + " to the collection " + collection + ".");			
		}
		
		client.commit(collection);
		
		logger.info("Added document list to the collection " + collection + " successfully.");		
	}
	
	public void deleteIndex(String collection, String id) throws SolrServerException, IOException
	{	
		client.deleteById(collection, id);
		
		client.commit(collection);	
		
		logger.info("Delete document with id " + id + " from the collection " + collection + ".");	
	}
	
	public void deleteIndex(String collection, List<String> ids) throws SolrServerException, IOException
	{	
		client.deleteById(collection, ids);
		
		client.commit(collection);	
		
		logger.info("Delete document with the input id list from the collection " + collection + ".");	
	}
	
	public void emptyCollection(String collection) throws SolrServerException, IOException
	{	
		client.deleteByQuery(collection, "*:*");
		
		client.commit(collection);	
		
		logger.info("Collection " + collection + " empty.");	
	}
		
}
