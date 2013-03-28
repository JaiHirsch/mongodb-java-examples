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
		builder.add("manufacturer","$manufacturer");
		builder.add("category","$category");
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
	
	

}
