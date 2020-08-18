package com.inforefiner.excuter

import com.inforefiner.excuter.CountRecordsExcuter.{getHDFSRecordsCount, getParquetRecordsCount, getallDataSetIdCollect, logger}
import com.inforefiner.helper.MerceDao
import com.inforefiner.util.{GZBase64Utils, HDFSClientUtils, LogsUtils}
import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object CountRecordsHdfs {

  LogsUtils.loadLog4jConf()
  val logger = Logger.getLogger(CountRecordsHdfs.getClass)

  def main(args: Array[String]): Unit = {

    logger.error(">>>>>>>>>>>>>>>>>流程任务，HDFS<<<<<<<<<<<<<<<<<")

    //初始化spark
    //      val sparkConf = new SparkConf().setAppName("count-records").setMaster("local[*]")
    val sparkConf = new SparkConf().setAppName("count-records")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)
    logger.error(">>>>>>>>>>>>>>>>>初始化Spark完成<<<<<<<<<<<<<<<<<")

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

      //HDFS的数据才处理
      if(storage.equals("HDFS")){

        //storage_configurations解码
        val uncomPath = GZBase64Utils.uncompressString(storageConfigurations)

        var path = ""
        var format = ""
        if(uncomPath != ""){
          val jsonObj: JSONObject = jsonParser.parse(uncomPath).asInstanceOf[JSONObject]
          val pathJson: AnyRef = jsonObj.get("path")
          val formatJson: AnyRef = jsonObj.get("format")
          if(pathJson != null && !pathJson.equals("")) path=pathJson.toString
          if(formatJson != null && !formatJson.equals("")) format=formatJson.toString
        }

        //判断hdfs文件是否存在
        if(HDFSClientUtils.existFileHDFS(path)){
          var i = 0l

          //判断文件是否是parquet类型
          if(format.equals("parquet")){
            //计算文件记录数
            var recordsCount = 0l
            //统计记录数
            try{
              //遍历取出目录下所有的文件并累加统计
              val files = HDFSClientUtils.getAllFiles(path)
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
                logger.error(">>>>>>统计HDFS中Parquet文件时，数据id为" + id + "，捕获异常：" + ex)
                //将非parquet文件记为错误文件，并记录个数并写入文件
                if(ex.toString.contains("is not a Parquet file")){
                  //如果不是parquet则是csv文件
                  //遍历取出目录下所有的文件并累加统计
                  val files = HDFSClientUtils.getAllFiles(path)
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
            //更新记录数
            MerceDao.updateDataSetFromId(id, recordsCount)
            i = recordsCount

          } else {
            //计算文件记录数
            var recordsCount = 0l
            try{
              //遍历取出目录下所有的文件并累加统计
              val files = HDFSClientUtils.getAllFiles(path)
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
              case ex: Exception => logger.error(">>>>>>统计HDFS非Parquet文件时，数据id为" + id + "，捕获异常：" + ex)
            }
            //更新记录数
            MerceDao.updateDataSetFromId(id, recordsCount)
            i = recordsCount

          }
          logger.error(dataSetName + "************" + id + ": ----" + i)
        }
      }
    }

    logger.error("**********统计完成**********")
  }
}
