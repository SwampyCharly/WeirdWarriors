package com.weirdwarriors.solrbasic.apps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.weirdwarriors.solrbasic.lib.*;

@SpringBootApplication
public class SolrApp1 
{	
	private static Logger logger = Logger.getLogger(SolrApp1.class);
	private static String zookeeperHost = "127.0.0.1:2181/solr";
	
	private static String requiredField = "data";
	
	private static String collection = "c1";
	private static String collection2 = "c2";
	private static String collection3 = "c3";

	public static void main(String[] args) 
	{
		SpringApplication.run(SolrApp1.class, args);
		
		logger.info("Starting application.");
		
		SolrInputDocument document1 = new SolrInputDocument();
		document1.addField("id", "1");
		document1.addField("name", "Punisher");
		document1.addField("price", "5.99");
		document1.addField(requiredField, "BLAM!");
		
		SolrInputDocument document2 = new SolrInputDocument();
		document2.addField("id", "2");
		document2.addField("name", "Spiderman");
		document2.addField("price", "4.99");
		document2.addField(requiredField, "TWHIP!");
		
		SolrInputDocument document3 = new SolrInputDocument();
		document3.addField("id", "3");
		document3.addField("name", "Deadpool");
		document3.addField("price", "3.99");
		document3.addField(requiredField, "BWAHAHAHA!");
		
		SolrInputDocument document4 = new SolrInputDocument();
		document4.addField("id", "4");
		document4.addField("name", "Wolverine");
		document4.addField("price", "2.99");
		document4.addField(requiredField, "SNIKT!");
		
		SolrInputDocument document5 = new SolrInputDocument();
		document5.addField("id", "5");
		document5.addField("name", "Nightcrawler");
		document5.addField("price", "1.99");
		document5.addField(requiredField, "BAMF!");
		
		SolrInputDocument document6 = new SolrInputDocument();
		document6.addField("id", "6");
		document6.addField("name", "Batman");
		document6.addField("price", "11.99");
		document6.addField(requiredField, "PAM!");
		
		try
		{				
			SolrIndexer solrIndexer = new SolrIndexer(zookeeperHost);
			
			solrIndexer.emptyCollection(collection);
			solrIndexer.emptyCollection(collection2);
			solrIndexer.emptyCollection(collection3);			
			
			solrIndexer.indexToCollection(collection, document1);
			solrIndexer.indexToCollection(collection, document2);
			solrIndexer.indexToCollection(collection, document3);
			solrIndexer.indexToCollection(collection, document4);
			solrIndexer.indexToCollection(collection, document5);
			solrIndexer.indexToCollection(collection, document6);	
			
			String queryString = "*:*";
			List<String> queryFields = new ArrayList<String>();
			queryFields.add("id");
			queryFields.add("name");
			queryFields.add("q1");
			
			String[] queryFieldsA = new String[3];
			queryFieldsA[0] = "id";
			queryFieldsA[1] = "name";
			queryFieldsA[2] = "q1";
			
			SolrAnalizer solrAnalizer = new SolrAnalizer(zookeeperHost);
			
			QueryResponse qr = solrAnalizer.analize(collection, queryString, queryFields);
			solrIndexer.indexToCollection(collection2, qr);
			
			QueryResponse qr2 = solrAnalizer.analize(collection, queryString, queryFieldsA);
			solrIndexer.indexToCollection(collection3, qr2);	
			
			SolrAnalizerSpark solrAnalizerSpark = new SolrAnalizerSpark(zookeeperHost);
			
			SolrDocumentList solrDocs = solrAnalizerSpark.analizeToList(collection, queryString, queryFieldsA);
			solrIndexer.indexToCollection(collection2, solrDocs);
			
			SolrDocumentList solrDocs2 = solrAnalizerSpark.analizeToList(collection, queryString, queryFields);
			solrIndexer.indexToCollection(collection3, solrDocs2);
			
			List<String> filters = new ArrayList<String>();
			filters.add("name=Spiderman");
			SolrDocumentList solrDocs3 = solrAnalizerSpark.analizeToList(collection, queryString, queryFields, filters);
			solrIndexer.indexToCollection(collection3, solrDocs3);
			
			solrIndexer.deleteIndex(collection3, "3");
			solrIndexer.deleteIndex(collection3, "1");
			
			solrIndexer.deleteIndex(collection2, "2");
			
			List<String> ids = new ArrayList<String>();
			ids.add("4");
			ids.add("5");
			
			solrIndexer.deleteIndex(collection, ids);	
			
			//solrIndexer.emptyCollection(collection);
			//solrIndexer.emptyCollection(collection2);
			//solrIndexer.emptyCollection(collection3);
			
			solrIndexer.closeConnection();
			solrAnalizer.closeConnection();
		} catch (SolrServerException e) 
		{
			logger.error(e.getMessage());
		} catch (IOException e) 
		{
			logger.error(e.getMessage());
		}
	}
}
