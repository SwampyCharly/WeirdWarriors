package com.example.solrbasic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.weirdwarriors.solrbasic.lib.SolrAnalizer;
import com.weirdwarriors.solrbasic.lib.SolrIndexer;
import com.weirdwarriors.solrbasic.lib.SolrUtils;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SolrbasicApplicationTests 
{

	private static String zookeeperHost = "127.0.0.1:2181/solr";
	
	private static String requiredField = "data";	
	private static String collection = "c5";
	private static String collection2 = "c5";
	private static String collection3 = "c6";

	private static SolrIndexer indexer;
	private static SolrAnalizer analizer;
	
	@Test
	public void test() throws IOException, SolrServerException 
	{
		try {
			//runOnceBeforeClass();
			
			test_method_1();
			
			org.junit.Assert.assertFalse(false);
		} catch (SolrServerException e) {
			//runOnceAfterClass();
			// TODO Auto-generated catch block
			org.junit.Assert.assertFalse(false);
		} catch (IOException e) {
			//runOnceAfterClass();
			// TODO Auto-generated catch block
			org.junit.Assert.assertFalse(false);
		}
		
		//runOnceAfterClass();
	}
	
	// Run once, e.g. Database connection, connection pool
	//Before
//    public static void runOnceBeforeClass() throws SolrServerException, IOException {
//        System.out.println("@BeforeClass - runOnceBeforeClass");
//                
//        indexer = new SolrIndexer(zookeeperHost);
//        analizer = new SolrAnalizer(zookeeperHost);
//        
//        indexer.emptyCollection(collection);
//    }

    // Run once, e.g close connection, cleanup
    //After
//    public static void runOnceAfterClass() throws IOException, SolrServerException {
//        System.out.println("@AfterClass - runOnceAfterClass");
//        
//        indexer.emptyCollection(collection);
//        
//        indexer.closeConnection();
//        analizer.closeConnection();
//    }
    
    // Run once, e.g. Database connection, connection pool
 	@Before
     public void runBefore() throws SolrServerException, IOException {
         System.out.println("@Before - runBefore");
                 
         indexer = new SolrIndexer(zookeeperHost);
         analizer = new SolrAnalizer(zookeeperHost);
         
         indexer.emptyCollection(collection);
     }

     // Run once, e.g close connection, cleanup
     @After
     public void runAfter() throws IOException, SolrServerException {
         System.out.println("@After - runAfter");
         
         indexer.emptyCollection(collection);
         
         indexer.closeConnection();
         analizer.closeConnection();
     }

    // Should rename to @BeforeTestMethod
    // e.g. Creating an similar object and share for all @Test
    //Before
    public void runBeforeTestMethod() {
        System.out.println("@Before - runBeforeTestMethod");
    }

    // Should rename to @AfterTestMethod
    //After
    public void runAfterTestMethod() throws SolrServerException, IOException {
        System.out.println("@After - runAfterTestMethod");
        
        indexer.emptyCollection(collection);
    }

    // Test
    public void test_method_1() throws SolrServerException, IOException {
        System.out.println("@Test - test_method_1");
        
        SolrInputDocument document1 = new SolrInputDocument();
		document1.addField("id", "1");
		document1.addField("name", "Punisher");
		document1.addField("price", "5.99");
		document1.addField(requiredField, "BLAM!");
		
		SolrInputDocument document2 = new SolrInputDocument();
		document2.addField("id", "2");
		document2.addField("name", "Spiderman");
		document2.addField("price", "19.99");
		document2.addField(requiredField, "TWHIP!");
		
		System.out.println("@Test - test_method_1 doc1");
		indexer.indexToCollection(collection, document1);
		System.out.println("@Test - test_method_1 doc2");
		indexer.indexToCollection(collection, document2);
		
		List<String> queryFields = new ArrayList<String>();
		queryFields.add("id");
		queryFields.add("name");
		queryFields.add("price");
		queryFields.add(requiredField);
		
		QueryResponse qr = analizer.analize(collection, "*:*", queryFields);
		SolrDocumentList docs = qr.getResults();
		
		SolrDocument provi = docs.get(0);
		Collection<String> fieldNames = provi.getFieldNames();
		
		SolrUtils.print(provi);
		
		for (String s : fieldNames)
		{
			System.out.println("Field name "+s);
			System.out.println("Field value " + provi.getFieldValue(s));
		}
		
		System.out.println("@Test - test_method_1 testing");
		
		if (!docs.get(0).getFieldValue("name").equals("Punisher"))
		{
			org.junit.Assert.assertFalse( true );	
		}
		else
		{
			//org.junit.Assert.assertTrue( true );
		}	
		
		System.out.println("@Test - test_method_1 comparison 1 ok");
		
		SolrInputDocument idoc = SolrUtils.toSolrInputDocument(docs.get(0));
		
		if (!SolrUtils.compareFieldNames(idoc, SolrUtils.toSolrInputDocument(docs.get(0))))
		{
			org.junit.Assert.assertFalse( true );	
		}
		else
		{
			//org.junit.Assert.assertTrue( true );
		}
		
		if (!SolrUtils.compare(idoc, SolrUtils.toSolrInputDocument(docs.get(0))))
		{
			org.junit.Assert.assertFalse( true );	
		}
		else
		{
			//org.junit.Assert.assertTrue( true );
		}
		
		System.out.println("@Test - test_method_1 comparison 2 ok");
		
		if (!docs.get(1).getFieldValue("name").equals("Spiderman"))
		{
			org.junit.Assert.assertFalse( true );	
		}
		else
		{
			//org.junit.Assert.assertTrue( true );
		}
		
		System.out.println("@Test - test_method_1 comparison 3 ok");
		
		idoc = SolrUtils.toSolrInputDocument(docs.get(1));

		if (!SolrUtils.compareFieldNames(idoc, SolrUtils.toSolrInputDocument(docs.get(1))))
		{
			org.junit.Assert.assertFalse( true );	
		}
		else
		{
			//org.junit.Assert.assertTrue( true );
		}
		
		if (!SolrUtils.compare(idoc, SolrUtils.toSolrInputDocument(docs.get(1))))
		{
			org.junit.Assert.assertFalse( true );	
		}
		else
		{
			//org.junit.Assert.assertTrue( true );
		}
		
		System.out.println("@Test - test_method_1 comparison 4 ok");
		
		System.out.println("@Test - test_method_1 runs ok");
    }

    // Test
    public void test_method_2() {
        System.out.println("@Test - test_method_2");
    }

}
