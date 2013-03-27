package com.examples.aggregation;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class AggregationExampleTest {

	AggregationExample	agg	= new AggregationExample();

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
			String[] array = iter.next().keySet().toArray(new String[] {});
			System.out.println(array[1]);
		}

		collection.drop();
	}

}
