/*
 * This is not actually intended to be a unit test, but a simple way to display the behavior of aggregation
 */

package com.examples.aggregation;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class AggregationExampleTest {
	private static AggregationExample	agg;
	private static MongoClient	client;
	private static DBCollection	collection;
	
	@BeforeClass
	public static void setUpMongo() {
		try {
			client = new MongoClient("localhost:27017");
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		DB db = client.getDB("agg");
		collection = db.getCollection("products");
		agg	= new AggregationExample();
		agg.setCollection(collection);
	}
	
	@AfterClass
	public static void closeMongo() {
		client.close();
	}


	@Test
	public void test() throws UnknownHostException {
		MongoClient client = new MongoClient("localhost:27017");
		DB db = client.getDB("agg");
		DBCollection collection = db.getCollection("products");
		List<String> vals = new ArrayList<String>();
		vals.add(RandomStringUtils.randomAlphanumeric(5));
		vals.add(RandomStringUtils.randomAlphanumeric(5));
		vals.add(RandomStringUtils.randomAlphanumeric(5));
		vals.add(RandomStringUtils.randomAlphanumeric(5));

		for (int i = 0; i < 100; i++) {
			if (i < 50)
				collection.insert(new BasicDBObject("manufacturer", vals.get(0)));
			else if(i < 75 ) collection.insert(new BasicDBObject("manufacturer", vals.get(1)));
			else if(i < 95 ) collection.insert(new BasicDBObject("manufacturer", vals.get(2)));
			else collection.insert(new BasicDBObject("manufacturer", vals.get(3)));
		}

		Iterator<DBObject> iter = agg.simpleAggregation();
		while(iter.hasNext()) {
			System.out.println(iter.next());
		}
		collection.drop();
		client.close();
	}
	
	

}
