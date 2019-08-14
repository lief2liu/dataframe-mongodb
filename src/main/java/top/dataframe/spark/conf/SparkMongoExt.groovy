package top.dataframe.spark.conf

import groovy.transform.CompileStatic
import top.dataframe.val.MongoInfoKey

@CompileStatic
class SparkMongoExt implements SparkConfExt {
    private Map<String, String> mongoInfo

    SparkMongoExt(Map<String, String> mongoInfo) {
        this.mongoInfo = mongoInfo
    }

    @Override
    SparkConfig enable(SparkConfig config) {
        String username = mongoInfo.get(MongoInfoKey.USER)
        String password = mongoInfo.get(MongoInfoKey.PASSWORD)
        String ipAndPort = mongoInfo.get(MongoInfoKey.IP_PORT)
        String db = mongoInfo.get(MongoInfoKey.DB)

        config
                .set("spark.mongodb.input.uri", "mongodb://$username:$password@$ipAndPort/admin")
                .set("spark.mongodb.output.uri", "mongodb://$username:$password@$ipAndPort/admin")
                .set("spark.mongodb.input.database", db)
                .set("spark.mongodb.output.database", db)

//                .config("spark.mongodb.input.collection", "")
    }
}
