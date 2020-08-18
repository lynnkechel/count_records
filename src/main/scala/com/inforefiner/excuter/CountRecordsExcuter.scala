package com.inforefiner.excuter

import org.apache.log4j.Logger
import com.inforefiner.helper.{MerceDao, PropertiesManager}
import com.inforefiner.model.{CollectInformation, CollectSameDataset, PathInformation}
import com.inforefiner.util.{DateTimeUtils, GZBase64Utils, HDFSClientUtils, LogsUtils}
import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, NotServingRegionException, TableName}
import org.apache.hadoop.hbase.client.{Admin, ConnectionFactory, NoServerForRegionException}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext, SparkException}


object CountRecordsExcuter {

    LogsUtils.loadLog4jConf()
    val logger = Logger.getLogger(CountRecordsExcuter.getClass)
  def main(args: Array[String]): Unit = {

    //获取开始时间
    val startTime = DateTimeUtils.getNowTime

    logger.error(">>>>>>>>>>>>>>>>>>>>>>>>当前时间：" + startTime)

    //json
    val jsonParser = new JSONParser()

    //通过时间判断当前是什么任务
//    val taskTags = PropertiesManager.getValue("task.number")
    val taskTags = args(0)

    logger.error(">>>>>>>>>>>>>>>>>>>>>>>>开始任务<<<<<<<<<<<<<<<<<<<<<<")

    //采集任务
    if(taskTags.equals("1")){

      logger.error(">>>>>>>>>>>>>>>>>采集任务<<<<<<<<<<<<<<<<<")

      val dataSetIdMap = new scala.collection.mutable.HashMap[String, CollectSameDataset]

      //统计采集数据记录数
      val collectCount = MerceDao.getCollectInfoCount()
      val collectInformationArray = MerceDao.getCollectInformation(collectCount)

      for(collectInformation <- collectInformationArray){

        //获取task_json并解析
        val pathJSON = collectInformation.taskJson
        val jsonObjTaskJson: JSONObject = jsonParser.parse(pathJSON).asInstanceOf[JSONObject]
        val dataStore: AnyRef = jsonObjTaskJson.get("dataStore")
        val jsonObjDataStore: JSONObject = jsonParser.parse(dataStore.toString).asInstanceOf[JSONObject]
        val modeJson = jsonObjDataStore.get("mode")
        val dataSetIdJson = jsonObjDataStore.get("id")
        var mode = ""
        var dataSetId = ""
        //获取mode判断是追加还是覆盖
        if(modeJson != null) {
          mode = modeJson.toString
        }
        //获取job_id
        if(dataSetIdJson != null){
          dataSetId = dataSetIdJson.toString
        }

        logger.error(">>>>>>>>>>" + dataSetId + "=========" + collectInformation.id)

        //判断是否有mode和id字段
        if(!mode.equals("") && !dataSetId.equals("")){
          //map存在公用的datasetID
          if(dataSetIdMap.contains(dataSetId)){
            //比较map中的endTime和当前endTime哪个是最新时间
            val compareValue = DateTimeUtils.compareTime(collectInformation.endTime, dataSetIdMap(dataSetId).endTime)
            //将最新的endTime更新到map中
            if(compareValue == 1){
              val collectSameDataset = CollectSameDataset(collectInformation.id, collectInformation.endTime)
              dataSetIdMap(dataSetId) = collectSameDataset
              //更新记录数
              updateDataSetRecordNumber(mode,dataSetId,collectInformation)
            }
          //不存在公用的datasetID直接添加到map中
          } else {
            val collectSameDataset = CollectSameDataset(collectInformation.id, collectInformation.endTime)
            dataSetIdMap += (dataSetId -> collectSameDataset)
            //更新记录数
            updateDataSetRecordNumber(mode,dataSetId,collectInformation)
          }
        }
      }
    }

    //流程任务HDFS
    if(taskTags.equals("2")){

      logger.error(">>>>>>>>>>>>>>>>>流程任务，HDFS<<<<<<<<<<<<<<<<<")

      //初始化spark
//      val sparkConf = new SparkConf().setAppName("count-records").setMaster("local[*]")
      val sparkConf = new SparkConf().setAppName("count-records")
      val sparkContext = new SparkContext(sparkConf)
      val sqlContext = new SQLContext(sparkContext)
      logger.error(">>>>>>>>>>>>>>>>>初始化Spark完成<<<<<<<<<<<<<<<<<")

      //错误文件个数
      var errorFileCount = 0
      //收集错误文件集合
      var errorFileList = List("文件名,文件路径,标注格式,实际格式")

      //获取所有数据集的路径信息
      //    val pathInformationArray = MerceDao.getDataSetPath()

      //获取所有采集数据的数据集id
      val allDataSetIdCollect = getallDataSetIdCollect

      //获取流程数据中hdfs数据并处理
      val pathInformationArrayHDFS = MerceDao.getFlowInformationHDFS(allDataSetIdCollect)

      for(pathInformation <- pathInformationArrayHDFS) {

        logger.error(pathInformation.id + "==" + pathInformation.storage_configurations)

        //JSON转换工具
        val pathJSON = pathInformation.storage_configurations

        //数据统计处理
        //数据存储类型是HDFS
        if(pathInformation.storage.equals("HDFS")){
          //解码
          val uncomPath = GZBase64Utils.uncompressString(pathJSON)

          //解析JSON格式，获取文件路径和文件格式
          var format = ""
          if(uncomPath != ""){
            val jsonObj: JSONObject = jsonParser.parse(uncomPath).asInstanceOf[JSONObject]
            val pathJson: AnyRef = jsonObj.get("path")
            val formatJson: AnyRef = jsonObj.get("format")
            if(pathJson != null && !pathJson.equals("")) pathInformation.storage_configurations_=(pathJson.toString)
            if(formatJson != null && !formatJson.equals("")) format=formatJson.toString
          }

          //判断hdfs文件是否存在
          if(HDFSClientUtils.existFileHDFS(pathInformation.storage_configurations)){

            //判断文件是否是parquet类型
            if(format.equals("parquet")){

              //计算文件记录数
              var recordsCount = 0l
              try{

                //遍历取出目录下所有的文件并累加统计
                val files = HDFSClientUtils.getAllFiles(pathInformation.storage_configurations)
                while(files.hasNext){
                  val next = files.next()
                  //如果是success文件则略过
                  if(!next.getPath.getName.equals("_SUCCESS")){
                    val filePath = next.getPath.toString
                    //累加结果
                    recordsCount = recordsCount + getParquetRecordsCount(sqlContext, filePath)
                  }
                }

              } catch {
                case ex: Exception =>

                  logger.error(">>>>>>统计HDFS中Parquet文件时，数据id为" + pathInformation.id + "，捕获异常：" + ex)

                  //将非parquet文件记为错误文件，并记录个数并写入文件
                  if(ex.toString.contains("is not a Parquet file")){
                    //记录数加一
                    errorFileCount = errorFileCount + 1
                    //将错误文件信息写入集合
                    errorFileList = errorFileList :+ pathInformation.name + "," + pathInformation.storage_configurations + ",parquet,csv"

                    //如果不是parquet则是csv文件
                    //遍历取出目录下所有的文件并累加统计
                    val files = HDFSClientUtils.getAllFiles(pathInformation.storage_configurations)
                    while(files.hasNext){
                      val next = files.next()
                      //如果是success文件则略过
                      if(!next.getPath.getName.equals("_SUCCESS")){
                        val filePath = next.getPath.toString
                        //累加结果
                        recordsCount = recordsCount + getHDFSRecordsCount(sparkContext, filePath)

                        logger.error(">>>>>已重新统计csv文件：" + recordsCount)
                      }
                    }
                  }
              }
              pathInformation.record_number_=(recordsCount)

              //将结果写入数据库
              MerceDao.updateRecords(pathInformation)

              //不是parquet类型
            } else {

              //计算文件记录数
              var recordsCount = 0l
              try{

                //遍历取出目录下所有的文件并累加统计
                val files = HDFSClientUtils.getAllFiles(pathInformation.storage_configurations)
                while(files.hasNext){
                  val next = files.next()
                  //如果是success文件则略过
                  if(!next.getPath.getName.equals("_SUCCESS")){
                    val filePath = next.getPath.toString
                    //累加结果
                    recordsCount = recordsCount + getHDFSRecordsCount(sparkContext, filePath)
                  }
                }

              } catch {
                case ex: Exception => logger.error(">>>>>>统计HDFS非Parquet文件时，数据id为" + pathInformation.id + "，捕获异常：" + ex)
              }
              pathInformation.record_number_=(recordsCount)

              //将结果写入数据库
              MerceDao.updateRecords(pathInformation)
            }
          }
        }

      }

      println(">>>>>>>>>>>开始写入错误文件<<<<<<<<<<<")
      //将错误文件写出
      //    val errorPath = System.getProperty("user.dir") + File.separator + "data" + File.separator + "error_file_record"
      val errorPath = "/tmp/rc"
      //将个数写入
      val countString = "个数总计：" + errorFileCount
      errorFileList = countString +: errorFileList
      val rdd = sparkContext.parallelize(errorFileList)
      rdd.saveAsTextFile(errorPath)
    }

    //流程任务HBASE
    if(taskTags.equals("3")){

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
      //      hbaseConf.set("hbase.regionserver.lease.period", "6000000")
      hbaseConf.set("hbase.client.scanner.timeout.period", "6000000")
      hbaseConf.set("zookeeper.session.timeout", "1800000")
      hbaseConf.set("hbase.zookeeper.property.maxClientCnxns", "600")
      hbaseConf.set("hbase.regionserver.handler.count", "100")
      val connection = ConnectionFactory.createConnection(hbaseConf)
      val admin: Admin = connection.getAdmin

      //获取所有采集数据的数据集id
      val allDataSetIdCollect = getallDataSetIdCollect

      //获取流程数据Hbase数据并处理
      val pathInformationArrayHBASE = MerceDao.getFlowInformationHBASE(allDataSetIdCollect)
      for(pathInformation <- pathInformationArrayHBASE){

        //JSON转换工具
        val jsonParser = new JSONParser()
        val pathJSON = pathInformation.storage_configurations

        //数据存储类型是HBASE
        if(pathInformation.storage.equals("HBASE")){

          if(PropertiesManager.getValue("hbase.switch").equals("on")){
            //解码
            val uncomPath = GZBase64Utils.uncompressString(pathJSON)

            logger.error(pathInformation.id + "------------标识---------------")

            //hbase命名空间
            var namespace = ""
            //解析JSON格式
            if(uncomPath != ""){
              val jsonObj: JSONObject = jsonParser.parse(uncomPath).asInstanceOf[JSONObject]
              val table = jsonObj.get("table")  //获取表明
              if(table != null && !table.equals("")) pathInformation.storage_configurations_=(table.toString)
              val namespaceJson = jsonObj.get("namespace")
              if(namespaceJson != null && !namespaceJson.equals("")) namespace = namespaceJson.toString
            }

            //计算文件记录数
            var recordsCount = 0l
            try{
              recordsCount = getHBaseRecordsCount(sparkContext, hbaseConf, pathInformation.storage_configurations, namespace, admin)
            } catch {
              case ex: IllegalArgumentException => logger.error(">>>>>>>>>>>不合法的表名称：" + pathInformation.storage_configurations)
              case ex: NoServerForRegionException => logger.error(">>>>>>>>>>>表不在server中：" + pathInformation.storage_configurations)
              case ex: NotServingRegionException => logger.error(">>>>>>>>>>>表中数据有损坏：" + pathInformation.storage_configurations)
              case ex: SparkException => logger.error(">>>>>>>>>>>表中数据有损坏：" + pathInformation.storage_configurations)
            }
            pathInformation.record_number_=(recordsCount)
            //将结果写入数据库
            MerceDao.updateRecords(pathInformation)
          }

        }
      }

      //关闭hbase资源
      admin.close()
      connection.close()
    }

    //获取结束时间
    val finishTime = DateTimeUtils.getNowTime

    logger.error("开始时间：" + startTime)
    logger.error("结束时间：" + finishTime)

  }


  /**
    * 获取所有采集数据的数据集id
    * @return
    */
  def getallDataSetIdCollect(): String = {
    val jsonParser = new JSONParser()
    //获取所有task_json并解析datasetId
    val taskJsonArray = MerceDao.getTaskJson()
    //所有采集数据的id
    var allDataSetIdCollectTemp = new StringBuffer()
    //解析出id并拼凑成字符转
    for (taskJson <- taskJsonArray) {
      val pathJSON = taskJson
      val jsonObjTaskJson: JSONObject = jsonParser.parse(pathJSON).asInstanceOf[JSONObject]
      val dataStore: AnyRef = jsonObjTaskJson.get("dataStore")
      val jsonObjDataStore: JSONObject = jsonParser.parse(dataStore.toString).asInstanceOf[JSONObject]
      val dataSetIdJson = jsonObjDataStore.get("id")
      //获取job_id
      var dataSetId = ""
      if(dataSetIdJson != null){
        dataSetId = "'" + dataSetIdJson.toString + "',"
      }
      allDataSetIdCollectTemp.append(dataSetId)
    }

    //所有采集数据的数据集id
    val allDataSetIdCollect = allDataSetIdCollectTemp.toString.dropRight(1)
    allDataSetIdCollect
  }


  /**
    * 更新记录数
    * @param mode
    * @param dataSetId
    * @param collectInformation
    */
  def updateDataSetRecordNumber(mode: String, dataSetId: String, collectInformation: CollectInformation) = {
    //如果是覆盖直接将记录数更新到库中
    if(mode.equals("overwrite")){
      val pathInformation = PathInformation(dataSetId,"","",collectInformation.writeOut.toLong,"")
      //更新记录数
      MerceDao.updateRecords(pathInformation)
      //如果是追加，需要累加所有的记录数更新到库
    } else if(mode.equals("append")){
      //更新记录数
      MerceDao.updateAppendCollectInfo(collectInformation.id, dataSetId)
    }
  }

  /**
    * 读取hdfs文件的记录数
    * @param sparkContext
    * @param filePath
    * @return
    */
  def getHDFSRecordsCount(sparkContext: SparkContext, filePath: String): Long = {
    val fileRDD: RDD[String] = sparkContext.textFile(filePath)
    fileRDD.count()
  }

  /**
    * 读取parquet文件的记录数
    * @param sqlContext
    * @param filePath
    * @return
    */
  def getParquetRecordsCount(sqlContext: SQLContext, filePath: String): Long = {
    val dataFrame = sqlContext.read.parquet(filePath)
    dataFrame.count()
  }

  /**
    * 读取HBase文件的记录数
    * @param sparkContext
    * @param hbaseConf
    * @param table
    * @param namespace
    * @param admin
    * @return
    */
  def getHBaseRecordsCount(sparkContext: SparkContext, hbaseConf: Configuration, table: String, namespace: String, admin: Admin): Long = {
//    val connection = ConnectionFactory.createConnection(hbaseConf)
//    val admin = connection.getAdmin
    var count = 0l
    if(namespace.equals("") || table.equals("")) return count
    //判断表是否存在
    if(admin.tableExists(TableName.valueOf(namespace + ":" + table))){
      //判断表是否开启
      if(!admin.isTableDisabled(TableName.valueOf(namespace + ":" + table))){
        admin
        hbaseConf.set(TableInputFormat.INPUT_TABLE, namespace + ":" + table)
        val hbaseRDD = sparkContext.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
          classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
          classOf[org.apache.hadoop.hbase.client.Result]
        )
        count = hbaseRDD.count()
      } else {
        logger.error(">>>>>>> 表" + table + " 已被禁用！" )
      }
    } else {
      logger.error(">>>>>>> 表" + table + " 不存在！" )
    }
//    admin.close()
//    connection.close()
    count
  }




}
