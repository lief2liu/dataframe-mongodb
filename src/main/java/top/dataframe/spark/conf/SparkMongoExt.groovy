//package top.dataframe.spark.conf
//
//import groovy.transform.CompileStatic
//
//@CompileStatic
//class SparkMongoExt implements SparkConfExt {
//    private Map<String, String> connInfo
//
//    SparkEsExt(Map<String, String> connInfo) {
//        this.connInfo = connInfo
//    }
//
//    @Override
//    SparkConfig enable(SparkConfig config) {
//        config.set("spark.mongodb.input.uri", "mongodb://$username:$password@$ipAndPort/admin")
//                .set("spark.mongodb.output.uri", "mongodb://$username:$password@$ipAndPort/admin")
//                .set("spark.mongodb.input.database", db)
//                .set("spark.mongodb.output.database", db)
//                .set("spark.mongodb.input.collection", "")
//
//
//        esInfo?.each {
//            config.set(it.key, it.value)
//        }
//
//        config
//    }
//}
