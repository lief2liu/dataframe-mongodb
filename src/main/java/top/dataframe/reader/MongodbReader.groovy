package top.dataframe.reader

import com.mongodb.ConnectionString
import com.mongodb.client.FindIterable
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoCursor
import com.mongodb.client.MongoDatabase
import groovy.transform.CompileStatic
import org.bson.Document
import top.dataframe.val.MongoInfoKey

@CompileStatic
class MongodbReader implements DataFrameReader, Serializable {
    private String match
    private List<String> cols
    private String collection
    private Map<String, Object> mongoInfo

    MongodbReader(Map<String, Object> mongoInfo, String collection, String match, List<String> cols) {
        this.match = match
        this.collection = collection
        this.cols = cols
        this.mongoInfo = mongoInfo
    }

    @Override
    List read() {
        String username = mongoInfo.get(MongoInfoKey.USER)
        String password = mongoInfo.get(MongoInfoKey.PASSWORD)
        String ipAndPort = mongoInfo.get(MongoInfoKey.IP_PORT)
        String db = mongoInfo.get(MongoInfoKey.DB)

        password = URLEncoder.encode(password, "utf-8")
        ConnectionString connString = new ConnectionString(
                "mongodb://$username:$password@$ipAndPort/admin"
        )
        MongoClient mongo

        try {
            mongo = MongoClients.create(connString)
            MongoDatabase mongodb = mongo.getDatabase(db)
            MongoCollection<Document> collection = mongodb.getCollection(collection)
            FindIterable findIterable = collection.find(Document.parse(match))
            MongoCursor<Document> mongoCursor = findIterable.iterator()

            List<Map<String, Object>> res = []
            while (mongoCursor.hasNext()) {
                Document doc = mongoCursor.next()
                Map<String, Object> row = [:]
                cols.each {
                    row.put(it, doc.get(it))
                }
                res.add(row)
            }

            if (res.size() > 0) {
                return [res, null]
            }

        } finally {
            mongo.close()
        }

        null
    }
}

