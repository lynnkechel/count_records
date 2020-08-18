import com.inforefiner.excuter.CountRecordsExcuter
import com.inforefiner.helper.{MerceDao, PropertiesManager}
import com.inforefiner.util.DateTimeUtils
import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Admin, ConnectionFactory}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object TestJson {

  def main(args: Array[String]): Unit = {

//    val jsonParser = new JSONParser()
//    val pathJSON = "{\"type\":\"SyncDataTask\",\"id\":\"\",\"name\":\"gbj_user_collector_for_merce_test\",\"createTime\":0,\"status\":0,\"taskType\":\"SYNC_DATA\",\"async\":false,\"exclusive\":false,\"type\":\"DataSyncTask\",\"collecterId\":\"c1\",\"schemaId\":\"31caabd3-ed37-415d-bc51-5c039f5b7689\",\"dataSource\":{\"type\":\"JDBC\",\"id\":\"8406b602-5352-4da9-a2df-1817e5dfe802\",\"name\":\"gbj_use_datasource_for_collector_c1_test\",\"type\":\"JDBC\",\"properties\":{},\"dbType\":\"\",\"driver\":\"com.mysql.jdbc.Driver\",\"url\":\"jdbc:mysql://192.168.1.189:3306/test\",\"username\":\"merce\",\"password\":\"merce\",\"catalog\":\"\",\"schema\":\"\",\"table\":\"student_info\",\"tableExt\":\"\",\"dateToTimestamp\":false,\"fetchSize\":0,\"queryTimeout\":0,\"object\":\"student_info\",\"readerName\":\"jdbc\"},\"dataStore\":{\"type\":\"HDFS\",\"id\":\"5ebd5da6-793d-4cf9-bb4a-f84301eb0c4e\",\"name\":\"gbj_use_students_short_84\",\"type\":\"HDFS\",\"fields\":[],\"path\":\"/tmp/gbj/datas_for_test/students_short_copy2.txt\",\"format\":\"csv\",\"separator\":\",\",\"mode\":\"overwrite\",\"sliceFormat\":\"\"},\"trigger\":\"0 0 16 ? * WED \",\"parallelism\":1,\"errorNumber\":0,\"stopOnSchemaChanged\":false,\"opts\":\"-Xss256k -Xms1G -Xmx1G -Xmn512M\",\"fieldMapping\":[{\"index\":0,\"sourceField\":\"sId\",\"sourceType\":\"\",\"targetField\":\"sId\",\"targetType\":\"string\"},{\"index\":0,\"sourceField\":\"sName\",\"sourceType\":\"\",\"targetField\":\"sName\",\"targetType\":\"string\"},{\"index\":0,\"sourceField\":\"sex\",\"sourceType\":\"string\",\"targetField\":\"sex\",\"targetType\":\"string\"},{\"index\":0,\"sourceField\":\"age\",\"sourceType\":\"int\",\"targetField\":\"age\",\"targetType\":\"int\"},{\"index\":0,\"sourceField\":\"class\",\"sourceType\":\"string\",\"targetField\":\"class\",\"targetType\":\"string\"}],\"cursorCol\":\"\",\"partitionKey\":\"\",\"partitionPattern\":\"\",\"bufferSize\":5000,\"flushPaddingTime\":30000,\"targetFields\":[\"sId\",\"sName\",\"sex\",\"age\",\"class\"],\"cursorIdx\":-1,\"partitionIdx\":-1,\"udfMap\":{},\"serviceType\":\"DTS\",\"cursorType\":\"\",\"sourceFields\":[\"sId\",\"sName\",\"sex\",\"age\",\"class\"],\"syncType\":\"data\"}"
//    val jsonObj: JSONObject = jsonParser.parse(pathJSON).asInstanceOf[JSONObject]
//    val path: AnyRef = jsonObj.get("dataStore")
//    val jsonObj1: JSONObject = jsonParser.parse(path.toString).asInstanceOf[JSONObject]
//    val path1: AnyRef = jsonObj1.get("mode")
//    println(path1)

//    val map =  new scala.collection.mutable.HashMap[String, ArrayBuffer[String]]
//    var a1 = ArrayBuffer("a1")
//    var a2 = ArrayBuffer("a2")
//    map += ("1" -> a1)
//    map += ("2" -> a1)
//
//
//
//    println(map("1")(0))
//
//    map("1")(0) = "b1"
//
//    println(map("1")(0))

//    var mode = ""
//    var dataSetId = ""
    //获取task_json并解析
//    val jsonParser = new JSONParser()
    //    val pathJSON = "{\"type\":\"SyncDataTask\",\"id\":\"\",\"name\":\"ftp_haitao_test_1\",\"createTime\":0,\"status\":0,\"taskType\":\"SYNC_DATA\",\"async\":false,\"exclusive\":false,\"type\":\"DataSyncTask\",\"collecterId\":\"WOVEN-SERVER\",\"dataSource\":{\"type\":\"FTP\",\"id\":\"7e1cdc81-dd19-4ae0-b1a8-672c179f26a2\",\"name\":\"ftp_haitao\",\"type\":\"FTP\",\"properties\":{},\"host\":\"192.168.1.84\",\"port\":21,\"username\":\"europa\",\"password\":\"europa\",\"fieldsSeparator\":\",\",\"dir\":\"ftp_auto_import\",\"filename\":\"idnameage.csv\",\"schemaId\":\"057889be-afc3-4b8f-ad97-d66ac4dad26f\",\"schemaName\":\"ftp\",\"object\":\"ftp_auto_import\",\"readerName\":\"ftp\"},\"dataStore\":{\"type\":\"HDFS\",\"id\":\"7b91d191-e6f9-4bfc-9911-83ce9b0bc133\",\"name\":\"ftp_haitao_test\",\"type\":\"HDFS\",\"fields\":[],\"path\":\"/tmp/ftp\",\"format\":\"csv\",\"separator\":\",\",\"mode\":\"append\"},\"trigger\":\"\",\"parallelism\":1,\"errorNumber\":0,\"stopOnSchemaChanged\":false,\"opts\":\"-Xss256k -Xms1G -Xmx1G -Xmn512M\",\"fieldMapping\":[{\"index\":0,\"sourceField\":\"id\",\"sourceType\":\"string\",\"targetField\":\"id\",\"targetType\":\"string\"},{\"index\":0,\"sourceField\":\"name\",\"sourceType\":\"string\",\"targetField\":\"name\",\"targetType\":\"string\"},{\"index\":0,\"sourceField\":\"age\",\"sourceType\":\"string\",\"targetField\":\"age\",\"targetType\":\"string\"}],\"cursorCol\":\"\",\"partitionKey\":\"\",\"partitionPattern\":\"\",\"bufferSize\":5000,\"flushPaddingTime\":30000,\"sourceFields\":[\"id\",\"name\",\"age\"],\"targetFields\":[\"id\",\"name\",\"age\"],\"cursorType\":\"\",\"cursorIdx\":-1,\"partitionIdx\":-1,\"udfMap\":{},\"syncType\":\"data\",\"serviceType\":\"DTS\"}"
    //    val jsonObjTaskJson: JSONObject = jsonParser.parse(pathJSON).asInstanceOf[JSONObject]

//    val dataStore: AnyRef = jsonObjTaskJson.get("fieldMapping")
//    jsonObjTaskJson.get
//    println(dataStore)
//    //获取mode判断是追加还是覆盖
//    if(modeJson != null) {
//      mode = modeJson.toString
//    }
//    //获取job_id
//    if(dataSetIdJson != null){
//      dataSetId = dataSetIdJson.toString
//    }
//
//     println(mode + "==" + dataSetId)
//    val jsonParser = new JSONParser()
//    val taskJsonArray = MerceDao.getTaskJson()
//    //所有采集数据的id
//    var allDataSetIdCollectTemp = new StringBuffer()
//    //解析出id并拼凑成字符转
//    for (taskJsjon <- taskJsonArray) {
//      val pathJSON = taskJson
//      val jsonObjTaskJson: JSONObject = jsonParser.parse(pathJSON).asInstanceOf[JSONObject]
//      val dataStore: AnyRef = jsonObjTaskJson.get("dataStore")
//      val jsonObjDataStore: JSONObject = jsonParser.parse(dataStore.toString).asInstanceOf[JSONObject]
//      val dataSetIdJson = jsonObjDataStore.get("id")
//      //获取job_id
//      var dataSetId = ""
//      if(dataSetIdJson != null){
//        dataSetId = ",'" + dataSetIdJson.toString + "'"
//      }
//      allDataSetIdCollectTemp.append(dataSetId)
//    }
//    val allDataSetIdCollect = allDataSetIdCollectTemp.toString.drop(1)
//    println(allDataSetIdCollectTemp)
//    println(allDataSetIdCollect)

//    val informations = MerceDao.getCollectInformation()
//
//    println(informations.length)
//
//    val informations = MerceDao.getCollectInformation()
//    for (elem <- informations) {
//      println(elem.writeOut)
//    }

//    val l = MerceDao.getCollectInfoRecordsCount("e80d3f55-16d4-40f4-a84e-b3bec5059a99")
//    println(l)


//    MerceDao.updateAppendCollectInfo("")

//    val str = DateTimeUtils.getNowTime()
//
//    println(str)
//    val jsonParser = new JSONParser()
//    val pathJSON = "{\"type\":\"SyncDataTask\",\"id\":\"\",\"name\":\"gbj_user_collector_for_merce_test\",\"createTime\":0,\"status\":0,\"taskType\":\"SYNC_DATA\",\"async\":false,\"exclusive\":false,\"type\":\"DataSyncTask\",\"collecterId\":\"c1\",\"schemaId\":\"31caabd3-ed37-415d-bc51-5c039f5b7689\",\"dataSource\":{\"type\":\"JDBC\",\"id\":\"8406b602-5352-4da9-a2df-1817e5dfe802\",\"name\":\"gbj_use_datasource_for_collector_c1_test\",\"type\":\"JDBC\",\"properties\":{},\"dbType\":\"\",\"driver\":\"com.mysql.jdbc.Driver\",\"url\":\"jdbc:mysql://192.168.1.189:3306/test\",\"username\":\"merce\",\"password\":\"merce\",\"catalog\":\"\",\"schema\":\"\",\"table\":\"student_info\",\"tableExt\":\"\",\"dateToTimestamp\":false,\"fetchSize\":0,\"queryTimeout\":0,\"object\":\"student_info\",\"readerName\":\"jdbc\"},\"dataStore\":{\"type\":\"HDFS\",\"id\":\"5ebd5da6-793d-4cf9-bb4a-f84301eb0c4e\",\"name\":\"gbj_use_students_short_84\",\"type\":\"HDFS\",\"fields\":[],\"path\":\"/tmp/gbj/datas_for_test/students_short_copy2.txt\",\"format\":\"csv\",\"separator\":\",\",\"mode\":\"overwrite\",\"sliceFormat\":\"\"},\"trigger\":\"0 0 16 ? * WED \",\"parallelism\":1,\"errorNumber\":0,\"stopOnSchemaChanged\":false,\"opts\":\"-Xss256k -Xms1G -Xmx1G -Xmn512M\",\"fieldMapping\":[{\"index\":0,\"sourceField\":\"sId\",\"sourceType\":\"\",\"targetField\":\"sId\",\"targetType\":\"string\"},{\"index\":0,\"sourceField\":\"sName\",\"sourceType\":\"\",\"targetField\":\"sName\",\"targetType\":\"string\"},{\"index\":0,\"sourceField\":\"sex\",\"sourceType\":\"string\",\"targetField\":\"sex\",\"targetType\":\"string\"},{\"index\":0,\"sourceField\":\"age\",\"sourceType\":\"int\",\"targetField\":\"age\",\"targetType\":\"int\"},{\"index\":0,\"sourceField\":\"class\",\"sourceType\":\"string\",\"targetField\":\"class\",\"targetType\":\"string\"}],\"cursorCol\":\"\",\"partitionKey\":\"\",\"partitionPattern\":\"\",\"bufferSize\":5000,\"flushPaddingTime\":30000,\"targetFields\":[\"sId\",\"sName\",\"sex\",\"age\",\"class\"],\"cursorIdx\":-1,\"partitionIdx\":-1,\"udfMap\":{},\"serviceType\":\"DTS\",\"cursorType\":\"\",\"sourceFields\":[\"sId\",\"sName\",\"sex\",\"age\",\"class\"],\"syncType\":\"data\"}"
//    val jsonObj: JSONObject = jsonParser.parse(jsonParser.parse(pathJSON).asInstanceOf[JSONObject].toString).asInstanceOf[JSONObject]
//
//    println(jsonObj)

//    val l = MerceDao.getCollectInfoCount()
//    println(l)
//    val informations = MerceDao.getCollectInformation(l)
//    var c = 1
//    for (elem <- informations) {
//      println(c + "==" + elem)
//      c+=1
//    }

//    var c = 1
//    val informations = CountRecordsExcuter.getallDataSetIdCollect()
////    for (elem <- informations) {
////      println(c + "==" +elem)
////      c += 1
////    }
//    println(informations)
//初始化spark
val sparkConf = new SparkConf().setAppName("count-records").setMaster("local[*]")
    //    val sparkConf = new SparkConf().setAppName("count-records")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)
    //初始化HBase
    val hbaseConf: Configuration = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", PropertiesManager.getValue("zookeeper.url"))
    hbaseConf.set("hbase.zookeeper.property.clientPort", PropertiesManager.getValue("zookeeper.port"))
    hbaseConf.set("zookeeper.znode.parent", "/hbase-unsecure")
    hbaseConf.set("hbase.regionserver.lease.period", "6000000")
    hbaseConf.set("zookeeper.session.timeout", "1800000")
    hbaseConf.set("hbase.zookeeper.property.maxClientCnxns", "600")
    hbaseConf.set("hbase.regionserver.handler.count", "100")
    val connection = ConnectionFactory.createConnection(hbaseConf)
    val admin: Admin = connection.getAdmin
    CountRecordsExcuter.getHBaseRecordsCount(sparkContext,hbaseConf,"","",admin)

  }

}
