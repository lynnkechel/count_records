package com.inforefiner.excuter

import com.inforefiner.excuter.CountRecordsExcuter.{getHBaseRecordsCount, logger}
import com.inforefiner.helper.{MerceDao, PropertiesManager}
import com.inforefiner.util.GZBase64Utils
import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, NotServingRegionException}
import org.apache.hadoop.hbase.client.{Admin, ConnectionFactory, NoServerForRegionException}
import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.apache.spark.sql.SQLContext

object TestHBase {

  def main(args: Array[String]): Unit = {

    logger.error(">>>>>>>>>>>>>>>>>流程任务，HBASE<<<<<<<<<<<<<<<<<")

    //初始化spark
    //      val sparkConf = new SparkConf().setAppName("count-records").setMaster("local[*]")
    val sparkConf = new SparkConf().setAppName("count-records")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)
    logger.error(">>>>>>>>>>>>>>>>>初始化Spark完成<<<<<<<<<<<<<<<<<")

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

//    val tablename = args(1)
//    val namespace = args(0)
//    logger.error("namespace:  " + namespace)
//    logger.error("table:  " + tablename)



    //取出所有有问题的数据集名，启动参数中配置（逗号分隔）
    val dataSetNameList: Array[String] = args(0).split(",")

    logger.error(args(0))

    for(dataSetName <- dataSetNameList){

      //通过数据集名获取该数据集id,storage,storage_configurations （id,storage_configurations,storage）
      val dataset = MerceDao.getDataSetFromName(dataSetName).split(",")
      val id = dataset(0)
      val storageConfigurations = dataset(1)
      val storage = dataset(2)

      //JSON转换工具
      val jsonParser = new JSONParser()

      //hbase的数据才处理
      if(storage.equals("HBASE")){

        //storage_configurations解码
        val uncomPath = GZBase64Utils.uncompressString(storageConfigurations)
        //解析json
        var table = ""
        var namespace = ""
        if(uncomPath != ""){
          val jsonObj: JSONObject = jsonParser.parse(uncomPath).asInstanceOf[JSONObject]
          val tableJson = jsonObj.get("table")  //获取表明
          if(tableJson != null && !tableJson.equals("")) table = tableJson.toString
          val namespaceJson = jsonObj.get("namespace")  //命名空间
          if(namespaceJson != null && !namespaceJson.equals("")) namespace = namespaceJson.toString
        }

        var recordsCount = 0l

        //统计记录数
        try{
          recordsCount = getHBaseRecordsCount(sparkContext, hbaseConf, table, namespace, admin)
        } catch {
          case ex: IllegalArgumentException => logger.error(">>>>>>>>>>>不合法的表名称：" + namespace + ":" + table)
          case ex: NoServerForRegionException => logger.error(">>>>>>>>>>>表不在server中：" +  namespace + ":" + table)
          case ex: NotServingRegionException => logger.error(">>>>>>>>>>>表中数据有损坏：" +  namespace + ":" + table)
          case ex: SparkException => logger.error(">>>>>>>>>>>表中数据有损坏：" +  namespace + ":" + table)
        }

        //更新记录数
        MerceDao.updateDataSetFromId(id, recordsCount)
        logger.error(dataSetName + "************" + namespace + ":" + table + "----" + recordsCount)
      }
    }


  }

}
