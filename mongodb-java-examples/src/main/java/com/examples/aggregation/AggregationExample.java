/*
 * ${file_name}
 * 
 * Copyright (c) 2013, Jai Hirsch. All rights reserved.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301  USA
 */

package com.examples.aggregation;

import java.util.Iterator;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class AggregationExample {
	private DBCollection	col;

	public void setCollection(DBCollection col) {
		this.col = col;
	}

	public Iterator<DBObject> simpleAggregation() {

		BasicDBObjectBuilder builder = new BasicDBObjectBuilder();
		builder.push("$group");
		builder.add("_id", "$manufacturer");
		builder.push("num_products");
		builder.add("$sum", 1);
		builder.pop();
		builder.pop();

		return col.aggregate(builder.get()).results().iterator();
	}

	public Iterator<DBObject> compoundAggregation() {
		BasicDBObjectBuilder builder = new BasicDBObjectBuilder();
		builder.push("$group");
		builder.push("_id");
		builder.add("manufacturer", "$manufacturer");
		builder.add("category", "$category");
		builder.pop();
		builder.push("num_products");
		builder.add("$sum", 1);
		builder.pop();
		builder.pop();

		return col.aggregate(builder.get()).results().iterator();
	}

	public Iterator<DBObject> sumPrices() {
		BasicDBObjectBuilder builder = new BasicDBObjectBuilder();
		builder.push("$group");
		builder.add("_id", "$manufacturer");
		builder.push("sum_prices");
		builder.add("$sum", "$price");
		builder.pop();
		builder.pop();

		return col.aggregate(builder.get()).results().iterator();
	}

	public Iterator<DBObject> averagePrices() {
		BasicDBObjectBuilder builder = new BasicDBObjectBuilder();
		builder.push("$group");
		builder.add("_id", "$category");
		builder.push("sum_prices");
		builder.add("$avg", "$price");
		builder.pop();
		builder.pop();

		return col.aggregate(builder.get()).results().iterator();
	}

	public Iterator<DBObject> addToSet() {
		BasicDBObjectBuilder builder = new BasicDBObjectBuilder();
		builder.push("$group");
		builder.push("_id");
		builder.add("maker", "$manufacturer");
		builder.pop();
		builder.push("categories");
		builder.add("$addToSet", "$category");
		builder.pop();
		builder.pop();

		return col.aggregate(builder.get()).results().iterator();
	}

	public Iterator<DBObject> push() {
		BasicDBObjectBuilder builder = new BasicDBObjectBuilder();
		builder.push("$group");
		builder.push("_id");
		builder.add("maker", "$manufacturer");
		builder.pop();
		builder.push("categories");
		builder.add("$push", "$category");
		builder.pop();
		builder.pop();

		return col.aggregate(builder.get()).results().iterator();

	}

	public Iterator<DBObject> maxPrice() {
		BasicDBObjectBuilder builder = new BasicDBObjectBuilder();
		builder.push("$group");
		builder.push("_id");
		builder.add("maker", "$manufacturer");
		builder.pop();
		builder.push("maxprice");
		builder.add("$max", "$price");
		builder.pop();
		builder.pop();

		return col.aggregate(builder.get()).results().iterator();

	}
	
	public Iterator<DBObject> doubleGroupStages() {
		BasicDBObjectBuilder group_1 = new BasicDBObjectBuilder();
		group_1.push("$group");
		group_1.push("_id");
		group_1.add("class_id", "$class_id");
		group_1.add("student_id", "$student_id");
		group_1.pop();
		group_1.push("average");
		group_1.add("$avg", "$score");
		group_1.pop();
		group_1.pop();
		
		BasicDBObjectBuilder group_2 = new BasicDBObjectBuilder();
		group_2.push("$group");
		group_2.add("_id","$_id.class_id");
		group_2.push("average");
		group_2.add("$avg", "$average");
		group_2.pop();
		group_2.pop();
		return col.aggregate(group_1.get(), group_2.get()).results().iterator();

	}

}
