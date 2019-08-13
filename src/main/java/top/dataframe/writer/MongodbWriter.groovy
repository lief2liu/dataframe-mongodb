package top.dataframe.writer

import com.mongodb.ConnectionString
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.BulkWriteOptions
import com.mongodb.client.model.UpdateOneModel
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.model.WriteModel
import groovy.json.JsonOutput
import groovy.transform.CompileStatic
import org.bson.Document
import top.dataframe.DataFrame
import top.dataframe.val.MongoInfoKey

@CompileStatic
class MongodbWriter implements DataFrameWriter {
    private Map<String, Object> mongoInfo
    private String collection

    MongodbWriter(Map<String, Object> mongoInfo, String collection) {
        this.mongoInfo = mongoInfo
        this.collection = collection
    }

    @Override
    int write(DataFrame df) {
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

            List<WriteModel<Document>> writeModelList = df.rows.collect {
                Object id = it.remove("_id")
                String filter = """ {"_id":$id} """.trim().toString()
                println filter
                String content = """ {"\$set": ${new JsonOutput().toJson(it)}} """
                println content

                new UpdateOneModel<Document>(
                        Document.parse(filter), Document.parse(content), new UpdateOptions().upsert(true)
                ) as WriteModel
            }
            mongodb.getCollection(collection).bulkWrite(writeModelList, new BulkWriteOptions().ordered(false))

            df.size()
        } finally {
            mongo.close()
        }
    }
}
