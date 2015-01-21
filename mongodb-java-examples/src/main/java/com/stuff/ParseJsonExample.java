package com.stuff;

import java.net.UnknownHostException;
import java.util.List;

import com.mongodb.AggregationOutput;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;

public class ParseJsonExample {

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws UnknownHostException {
        MongoClient mc = null;
        try {
            mc = new MongoClient();
            DBCollection col = mc.getDB("students").getCollection("grades");

            // simple DBObject parse:

            Object simpleParse = JSON.parse("{type:'exam',score:{$gt:65, $lt:75}}");

            DBCursor simpleFind = col.find((DBObject) simpleParse).limit(5);
            while (simpleFind.hasNext()) {
                System.out.println(simpleFind.next());
            }
            simpleFind.close();

            // complex DBList parse:
            Object parse = JSON
                    .parse("[{'$group':{'_id':'$student_id', 'average':{$avg:'$score'}}}, {'$sort':{'average':-1}}, {'$limit':1}]");
            if (parse instanceof List<?>) {
                List<DBObject> query = (List<DBObject>) parse;

                AggregationOutput aggregate = col.aggregate(query);
                System.out.println(aggregate.results().iterator().next());
            } else {
                System.out.println("Unexpected result from JSON parse, expected List<?> but was "
                        + parse.getClass().getName());
            }
        } finally {
            if (null != mc)
                mc.close();
        }

    }

}
