/*
 * This is not actually intended to be a unit test, but a simple way to display the behavior of aggregation
 */

package com.examples.aggregation;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;
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
	private static DB	db;
	
	@BeforeClass
	public static void setUpMongo() {
		try {
			client = new MongoClient("localhost:27017");
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		db = client.getDB("agg");
		collection = db.getCollection("products");
		agg	= new AggregationExample();
		agg.setCollection(collection);
	}
	
	@AfterClass
	public static void closeMongo() {
		client.close();
	}


	@Test
	public void simpleAggregation() throws UnknownHostException {
		List<String> vals = generateRandomAlphaNumericValues();

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
	}
	
	@Test
	public void compoundGrouping() {
		List<String> manufacturers = generateRandomAlphaNumericValues();
		List<String> categories = generateRandomAlphaNumericValues();
		
		for (int i = 0; i < 100; i++) {
			if (i < 50)
				collection.insert(new BasicDBObject("manufacturer", manufacturers.get(0)).append("category", categories.get(RandomUtils.nextInt(categories.size()))));
			else if(i < 75 ) collection.insert(new BasicDBObject("manufacturer", manufacturers.get(1)).append("category", categories.get(RandomUtils.nextInt(categories.size()))));
			else if(i < 95 ) collection.insert(new BasicDBObject("manufacturer", manufacturers.get(2)).append("category", categories.get(RandomUtils.nextInt(categories.size()))));
			else collection.insert(new BasicDBObject("manufacturer", manufacturers.get(3)).append("category", categories.get(RandomUtils.nextInt(categories.size()))));
		}
		
		Iterator<DBObject> iter = agg.compoundAggregation();
		while(iter.hasNext()) {
			System.out.println(iter.next());
		}
		collection.drop();
	}
	
	@Test
	public void usingSumTest() {
		List<String> manufacturers = generateRandomAlphaNumericValues();
		List<Double> prices = generateRandomDoubleValues();
		
		for (int i = 0; i < 100; i++) {
			if (i < 50)
				collection.insert(new BasicDBObject("manufacturer", manufacturers.get(0)).append("price", prices.get(RandomUtils.nextInt(prices.size()))));
			else if(i < 75 ) collection.insert(new BasicDBObject("manufacturer", manufacturers.get(1)).append("price", prices.get(RandomUtils.nextInt(prices.size()))));
			else if(i < 95 ) collection.insert(new BasicDBObject("manufacturer", manufacturers.get(2)).append("price", prices.get(RandomUtils.nextInt(prices.size()))));
			else collection.insert(new BasicDBObject("manufacturer", manufacturers.get(3)).append("price", prices.get(RandomUtils.nextInt(prices.size()))));
		}
		
		Iterator<DBObject> iter = agg.sumPrices();
		while(iter.hasNext()) {
			System.out.println(iter.next());
		}
		collection.drop();
		
	}
	
	@Test
	public void usingAvgTest() {
		List<String> categories = generateRandomAlphaNumericValues();
		List<Double> prices = generateRandomDoubleValues();
		
		for (int i = 0; i < 100; i++) {
			if (i < 50)
				collection.insert(new BasicDBObject("category", categories.get(0)).append("price", prices.get(RandomUtils.nextInt(prices.size()))));
			else if(i < 75 ) collection.insert(new BasicDBObject("category", categories.get(1)).append("price", prices.get(RandomUtils.nextInt(prices.size()))));
			else if(i < 95 ) collection.insert(new BasicDBObject("category", categories.get(2)).append("price", prices.get(RandomUtils.nextInt(prices.size()))));
			else collection.insert(new BasicDBObject("category", categories.get(3)).append("price", prices.get(RandomUtils.nextInt(prices.size()))));
		}
		
		Iterator<DBObject> iter = agg.averagePrices();
		while(iter.hasNext()) {
			System.out.println(iter.next());
		}
		collection.drop();
		
	}
	
	@Test
	public void addToSetTest() {
		List<String> manufacturers = generateRandomAlphaNumericValues();
		List<String> categories = generateRandomAlphaNumericValues();
		
		for (int i = 0; i < 100; i++) {
			if (i < 50)
				collection.insert(new BasicDBObject("manufacturer", manufacturers.get(0)).append("category", categories.get(RandomUtils.nextInt(categories.size()))));
			else if(i < 75 ) collection.insert(new BasicDBObject("manufacturer", manufacturers.get(1)).append("category", categories.get(RandomUtils.nextInt(categories.size()))));
			else if(i < 95 ) collection.insert(new BasicDBObject("manufacturer", manufacturers.get(2)).append("category", categories.get(RandomUtils.nextInt(categories.size()))));
			else collection.insert(new BasicDBObject("manufacturer", manufacturers.get(3)).append("category", categories.get(RandomUtils.nextInt(categories.size()))));
		}
		
		Iterator<DBObject> iter = agg.addToSet();
		while(iter.hasNext()) {
			System.out.println(iter.next());
		}
		collection.drop();
	}
	
	@Test
	public void pushTest() {
		List<String> manufacturers = generateRandomAlphaNumericValues();
		List<String> categories = generateRandomAlphaNumericValues();
		
		for (int i = 0; i < 100; i++) {
			if (i < 50)
				collection.insert(new BasicDBObject("manufacturer", manufacturers.get(0)).append("category", categories.get(RandomUtils.nextInt(categories.size()))));
			else if(i < 75 ) collection.insert(new BasicDBObject("manufacturer", manufacturers.get(1)).append("category", categories.get(RandomUtils.nextInt(categories.size()))));
			else if(i < 95 ) collection.insert(new BasicDBObject("manufacturer", manufacturers.get(2)).append("category", categories.get(RandomUtils.nextInt(categories.size()))));
			else collection.insert(new BasicDBObject("manufacturer", manufacturers.get(3)).append("category", categories.get(RandomUtils.nextInt(categories.size()))));
		}
		
		Iterator<DBObject> iter = agg.push();
		while(iter.hasNext()) {
			System.out.println(iter.next());
		}
		collection.drop();
	}

	@Test
	public void maxAndMinTest() {
		List<String> manufacturers = generateRandomAlphaNumericValues();
		List<Double> prices = generateRandomDoubleValues();
		
		for (int i = 0; i < 100; i++) {
			if (i < 50)
				collection.insert(new BasicDBObject("manufacturer", manufacturers.get(0)).append("price", prices.get(RandomUtils.nextInt(prices.size()))));
			else if(i < 75 ) collection.insert(new BasicDBObject("manufacturer", manufacturers.get(1)).append("price", prices.get(RandomUtils.nextInt(prices.size()))));
			else if(i < 95 ) collection.insert(new BasicDBObject("manufacturer", manufacturers.get(2)).append("price", prices.get(RandomUtils.nextInt(prices.size()))));
			else collection.insert(new BasicDBObject("manufacturer", manufacturers.get(3)).append("price", prices.get(RandomUtils.nextInt(prices.size()))));
		}
		
		Iterator<DBObject> iter = agg.maxPrice();
		while(iter.hasNext()) {
			System.out.println(iter.next());
		}
		collection.drop();
	}
	
	@Test
	public void doubleGroupStages() {
		DBCollection grades = db.getCollection("grades");
		agg.setCollection(grades);
		int[] class_id = {1,2,3,4,5};
		int[] student_id = {100,101,102,103,104,105};
		for(int cls = 0; cls < class_id.length; cls++) {
			for(int student = 0; student < student_id.length; student++) {
				grades.insert(new BasicDBObject("class_id", class_id[cls]).append("student_id", student_id[student] ).append("score", RandomUtils.nextInt(100)));
			}
		}
		Iterator<DBObject> iter = agg.doubleGroupStages();
		while(iter.hasNext()) {
			System.out.println(iter.next());
		}
		grades.drop();
	}
	
	@Test
	public void projectTest() {
		collection.insert(new BasicDBObject("manufacturer", "Apple").append("price", 2000.01).append("category", "Lap Top").append("name", "MacBookAir"));
		Iterator<DBObject> iter = agg.project();
		while(iter.hasNext()) {
			System.out.println(iter.next());
		}
		collection.drop();
	}
	
	@Test
	public void matchTest() {
		// this test assumes that you have imported the zips.jason file provided in the M101J course from 10Gen, week 5 homework
		DBCollection zipsCollection = client.getDB("states").getCollection("zips");
		agg.setCollection(zipsCollection);
		Iterator<DBObject> iter = agg.match();
		while(iter.hasNext()) {
			System.out.println(iter.next());
		}
	}
	
	@Test
	public void sortTest() {
		DBCollection zipsCollection = client.getDB("states").getCollection("zips");
		agg.setCollection(zipsCollection);
		Iterator<DBObject> iter = agg.sort();
		while(iter.hasNext()) {
			System.out.println(iter.next());
		}
	}
	
	@Test
	public void limitAndSkipTest() {
		DBCollection zipsCollection = client.getDB("states").getCollection("zips");
		agg.setCollection(zipsCollection);
		Iterator<DBObject> iter = agg.limitAndSkip();
		while(iter.hasNext()) {
			System.out.println(iter.next());
		}
	}

	private List<Double> generateRandomDoubleValues() {
		List<Double> prices = new ArrayList<Double>();
		for(int i = 0; i<5; i++) {
			Double d = Double.valueOf(String.format("%.2f", (RandomUtils.nextDouble() * RandomUtils.nextInt(500))));
			prices.add(d);
			
		}
		return prices;
	}

	private List<String> generateRandomAlphaNumericValues() {
		List<String> vals = new ArrayList<String>();
		vals.add(RandomStringUtils.randomAlphanumeric(5));
		vals.add(RandomStringUtils.randomAlphanumeric(5));
		vals.add(RandomStringUtils.randomAlphanumeric(5));
		vals.add(RandomStringUtils.randomAlphanumeric(5));
		return vals;
	}
	
	

}
