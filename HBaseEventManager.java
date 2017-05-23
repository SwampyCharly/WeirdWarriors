package com.weirdwarriors.hbasebasic.apps;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import kafka.serializer.StringDecoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.weirdwarriors.hbasebasic.lib.*;

public class HBaseEventManager 
{
	private static Logger logger = Logger.getLogger(HBaseEventManager.class);
	private static String zookeeperPort = "2181";
	private static String zookeeperNodeHBase = "/hbase";
	
	private static String namespaceString = "nscr";
	private static String eventsTableString = "events";
	private static String usersAndEventsTableString = "usersnevents";
	private static String eventsTopicString = "eventsTopic";
	private static String HDFSPath = "/user/cloudera/cr/";
	private static String columnFamilyString = "f";	
	private static String separator = "\t";
	private static String hostName;
	private static String kafkaPort = "9092";
	private static Configuration HBaseConfig;
	private static JavaSparkContext context;
    
    public static void main( String[] args )
    {
    	try 
    	{   
    		HBaseManager hbm = new HBaseManager(zookeeperPort, zookeeperNodeHBase);
    		
    		hbm.createTable(namespaceString, eventsTableString);
    		hbm.createTable(namespaceString, usersAndEventsTableString);
    		
    		hostName = InetAddress.getLocalHost().getHostName();
    		
    		HBaseConfig = HBaseConfiguration.create();
    		HBaseConfig.set("hbase.zookeeper.quorum", hostName);
    		HBaseConfig.set("hbase.zookeeper.property.clientPort", zookeeperPort);
    		HBaseConfig.set("zookeeper.znode.parent", zookeeperNodeHBase);	
    		
    		logger.info("HBase configuration created and ImportToHBase instanced.");
    		
    		SparkConf conf = new SparkConf().setAppName("ImportToHBase").setMaster("local[*]");
    		context = new JavaSparkContext(conf);
    		JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.milliseconds(500));
    		
    		String kafkaBrokerString = hostName + ":" + kafkaPort; 
    		
    		Map<String, String> kafkaParams = new HashMap<String, String>();
    		kafkaParams.put("metadata.broker.list", kafkaBrokerString);
    		
    		Set<String> topics = Collections.singleton(eventsTopicString);
    		
    		JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);
    		 
    		Properties props = new Properties();
    		props.put("bootstrap.servers", kafkaBrokerString);
    		props.put("client.id", "KafkaProducer");
    		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    		
    		KafkaProducer producer = new KafkaProducer(props);
    		
    		
    		
//    		ProducerRecord<String, String> record = new ProducerRecord<String, String>(eventsTopicString, "value!");
    		try
    		{
    			ssc.start();
	    	    ssc.awaitTermination();
    		}
    		finally
    		{
    			producer.close();
    		}
		} catch (IOException e) 
    	{
			e.printStackTrace();
		}
    }
    
}
