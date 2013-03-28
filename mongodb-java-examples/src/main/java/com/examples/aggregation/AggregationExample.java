/*
 * ${file_name}
 * 
 * Copyright (c) 2013, Jai Hirsch. All rights reserved.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
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

import java.net.UnknownHostException;
import java.util.Iterator;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class AggregationExample {
	private DBCollection	col;

	public void setCollection(DBCollection col) {
		this.col = col;
	}

	public Iterator<DBObject> simpleAggregation() throws UnknownHostException {

		BasicDBObjectBuilder builder = new BasicDBObjectBuilder();
		builder.push("$group");
		builder.add("_id", "$manufacturer");
		builder.push("num_products");
		builder.add("$sum", 1);
		builder.pop();
		builder.pop();

		return col.aggregate(builder.get()).results().iterator();
	}

}
