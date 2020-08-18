package com.inforefiner.helper

import java.sql.ResultSet

import com.inforefiner.model.{CollectInformation, PathInformation}

import scala.collection.mutable.ArrayBuffer

object MerceDao {


  /**
    * 通过数据集名获取该数据集id,storage,storage_configurations
    * @param dataSetName
    * @return
    */
  def getDataSetFromName(dataSetName: String): String = {

    val sql = "SELECT id, storage_configurations, storage FROM merce_dataset WHERE name = '" + dataSetName + "'"

    val mySqlPool = CreateMySqlPool()
    val client = mySqlPool.borrowObject()

    var resultStr = ""
    client.executeQuery(sql, null, new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while(rs.next()){
          val id = rs.getString(1)
          val storage_configurations = rs.getString(2)
          val storage = rs.getString(3)
          resultStr = id + "," + storage_configurations + "," + storage
        }
      }
    })

    mySqlPool.returnObject(client)

    return resultStr
  }


  /**
    * 获取所有task_json
    * @return
    */
  def getTaskJson(): Array[String] = {

    val sql = "SELECT task_json FROM " + PropertiesManager.getValue("table.sync_job")

    val mySqlPool = CreateMySqlPool()
    val client = mySqlPool.borrowObject()

    val taskJsonArray = new ArrayBuffer[String]()
    client.executeQuery(sql, null, new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while(rs.next()){
          val taskJson = rs.getString(1)
          taskJsonArray += taskJson
        }
      }
    })

    mySqlPool.returnObject(client)

    taskJsonArray.toArray
  }


  /**
    * 获取采集数据的数据量
    * @return
    */
  def getCollectInfoCount(): Long = {

    val sql = "SELECT COUNT(*) FROM " + PropertiesManager.getValue("table.merce_execution_task")

    val mySqlPool = CreateMySqlPool()
    val client = mySqlPool.borrowObject()

    var count = 0l
    client.executeQuery(sql, null, new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while(rs.next()){
          count = rs.getLong(1)
        }
      }
    })

    mySqlPool.returnObject(client)

    count
  }


  /**
    * 获取流程数据(HBASE)
    * @return
    */
  def getFlowInformationHBASE(dataSetIDs: String): Array[PathInformation] = {

    val sql = "SELECT id,storage,storage_configurations,record_number,name " +
      "FROM "  + PropertiesManager.getValue("table.name") + " WHERE id NOT IN (" + dataSetIDs + ")" +
      "AND storage = 'HBASE'"

    val mySqlPool = CreateMySqlPool()
    val client = mySqlPool.borrowObject()

    val pathInformationArray = new ArrayBuffer[PathInformation]()
    client.executeQuery(sql, null, new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while(rs.next()){
          val id = rs.getString(1)
          val storage = rs.getString(2)
          val storage_configurations = rs.getString(3)
          val name = rs.getString(4)
          pathInformationArray += PathInformation(id, storage, storage_configurations, 0l, name)
        }
      }
    })

    mySqlPool.returnObject(client)

    pathInformationArray.toArray
  }


  /**
    * 获取流程数据(HDFS)
    * @return
    */
  def getFlowInformationHDFS(dataSetIDs: String): Array[PathInformation] = {

    val sql = "SELECT id,storage,storage_configurations,record_number,name " +
      "FROM "  + PropertiesManager.getValue("table.name") + " WHERE id NOT IN (" + dataSetIDs + ")" +
      "AND storage = 'HDFS'"

    val mySqlPool = CreateMySqlPool()
    val client = mySqlPool.borrowObject()

    val pathInformationArray = new ArrayBuffer[PathInformation]()
    client.executeQuery(sql, null, new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while(rs.next()){
          val id = rs.getString(1)
          val storage = rs.getString(2)
          val storage_configurations = rs.getString(3)
          val name = rs.getString(4)
          pathInformationArray += PathInformation(id, storage, storage_configurations, 0l, name)
        }
      }
    })

    mySqlPool.returnObject(client)

    pathInformationArray.toArray
  }


  /**
    * 单个更新记录数
    * @param darSetId
    * @param recordNumber
    */
  def updateDataSetFromId(darSetId: String, recordNumber: Long) = {

    val sql = "UPDATE " + PropertiesManager.getValue("table.name") + " SET record_number = ? WHERE id = ?"

    val mySqlPool = CreateMySqlPool()
    val client = mySqlPool.borrowObject()

    client.executeUpdate(sql, Array(recordNumber, darSetId))

    mySqlPool.returnObject(client)
  }


  /**
    * 像库中更新采集数据中为追加数据的记录数
    * @param jobId
    * @param dataSetId
    */
  def updateAppendCollectInfo(jobId: String, dataSetId: String) = {

    val sql = "UPDATE " + PropertiesManager.getValue("table.name") + " SET record_number = " +
      "(SELECT SUM(write_out) FROM " + PropertiesManager.getValue("table.merce_execution_task") + " WHERE " +
        "job_id = '" + jobId + "') WHERE id = '" + dataSetId + "'"

    val mySqlPool = CreateMySqlPool()
    val client = mySqlPool.borrowObject()

    client.executeUpdate(sql, null)

    mySqlPool.returnObject(client)
  }


  /**
    * 获取采集数据
    * @return
    */
  def getCollectInformation(collectCount: Long): Array[CollectInformation] = {

    val sql = "SELECT s.id,s.task_json,t.write_out,t.end_time FROM " + PropertiesManager.getValue("table.sync_job") + " s INNER JOIN " +
      "(SELECT  task.* FROM " +
      "(SELECT job_id,write_out,end_time,status FROM " + PropertiesManager.getValue("table.merce_execution_task") + " ORDER BY end_time DESC " +
      "LIMIT " + collectCount + ") task GROUP BY job_id) t " +
      "ON s.id = t.job_id WHERE s.status = '1' AND t.status = '2'"

    val collectInformationArray = new ArrayBuffer[CollectInformation]()

    val mySqlPool = CreateMySqlPool()
    val client = mySqlPool.borrowObject()

    client.executeQuery(sql, null, new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while(rs.next()){
          val id = rs.getString(1)
          val taskJson = rs.getString(2)
          val writeOut = rs.getString(3)
          val endTime = rs.getString(4)
          collectInformationArray += CollectInformation(id, taskJson, writeOut, endTime)
        }
      }
    })

    mySqlPool.returnObject(client)

    collectInformationArray.toArray
  }


  /**
    * 获取数据集
    * @return
    */
  def getDataSetPath(): Array[PathInformation] = {

    val sql = "SELECT id, storage, storage_configurations, name FROM " + PropertiesManager.getValue("table.name")

    val pathInformationArray = new ArrayBuffer[PathInformation]()

    val mySqlPool = CreateMySqlPool()
    val client = mySqlPool.borrowObject()

    client.executeQuery(sql, null, new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while(rs.next()){
          val id = rs.getString(1)
          val storage = rs.getString(2)
          val storage_configurations = rs.getString(3)
          val name = rs.getString(4)
          pathInformationArray += PathInformation(id, storage, storage_configurations, 0l, name)
        }
      }
    })

    mySqlPool.returnObject(client)

    pathInformationArray.toArray
  }



  /***
    *
    *
    *                                         ,s555SB@@&
    *                                      :9H####@@@@@Xi
    *                                     1@@@@@@@@@@@@@@8
    *                                   ,8@@@@@@@@@B@@@@@@8
    *                                  :B@@@@X3hi8Bs;B@@@@@Ah,
    *             ,8i                  r@@@B:     1S ,M@@@@@@#8;
    *            1AB35.i:               X@@8 .   SGhr ,A@@@@@@@@S
    *            1@h31MX8                18Hhh3i .i3r ,A@@@@@@@@@5
    *            ;@&i,58r5                 rGSS:     :B@@@@@@@@@@A
    *             1#i  . 9i                 hX.  .: .5@@@@@@@@@@@1
    *              sG1,  ,G53s.              9#Xi;hS5 3B@@@@@@@B1
    *               .h8h.,A@@@MXSs,           #@H1:    3ssSSX@1
    *               s ,@@@@@@@@@@@@Xhi,       r#@@X1s9M8    .GA981
    *               ,. rS8H#@@@@@@@@@@#HG51;.  .h31i;9@r    .8@@@@BS;i;
    *                .19AXXXAB@@@@@@@@@@@@@@#MHXG893hrX#XGGXM@@@@@@@@@@MS
    *                s@@MM@@@hsX#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@&,
    *              :GB@#3G@@Brs ,1GM@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@B,
    *            .hM@@@#@@#MX 51  r;iSGAM@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@8
    *          :3B@@@@@@@@@@@&9@h :Gs   .;sSXH@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@:
    *      s&HA#@@@@@@@@@@@@@@M89A;.8S.       ,r3@@@@@@@@@@@@@@@@@@@@@@@@@@@r
    *   ,13B@@@@@@@@@@@@@@@@@@@5 5B3 ;.         ;@@@@@@@@@@@@@@@@@@@@@@@@@@@i
    *  5#@@#&@@@@@@@@@@@@@@@@@@9  .39:          ;@@@@@@@@@@@@@@@@@@@@@@@@@@@;
    *  9@@@X:MM@@@@@@@@@@@@@@@#;    ;31.         H@@@@@@@@@@@@@@@@@@@@@@@@@@:
    *   SH#@B9.rM@@@@@@@@@@@@@B       :.         3@@@@@@@@@@@@@@@@@@@@@@@@@@5
    *     ,:.   9@@@@@@@@@@@#HB5                 .M@@@@@@@@@@@@@@@@@@@@@@@@@B
    *           ,ssirhSM@&1;i19911i,.             s@@@@@@@@@@@@@@@@@@@@@@@@@@S
    *              ，,rHAri1h1rh&@#353Sh:          8@@@@@@@@@@@@@@@@@@@@@@@@@#:
    *            .A3hH@#5S553&@@#h   i:i9S          #@@@@@@@@@@@@@@@@@@@@@@@@@A.
    *
    *
    *    又看源码，看你妹呀！
    *
    *
    */



  /**
    * 更新记录数
    * @param pathInformation
    */
  def updateRecords(pathInformation: PathInformation) ={

    val sql = "UPDATE " + PropertiesManager.getValue("table.name") + " SET record_number = ? WHERE id = ?"

    // 获取对象池单例对象
    val mySqlPool = CreateMySqlPool()
    // 从对象池中提取对象
    val client = mySqlPool.borrowObject()

    client.executeUpdate(sql, Array(pathInformation.record_number, pathInformation.id))

    // 使用完成后将对象返回给对象池
    mySqlPool.returnObject(client)
  }

}
