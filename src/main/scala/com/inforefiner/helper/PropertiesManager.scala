package com.inforefiner.helper

import java.io.{File, FileInputStream}
import java.util.Properties

object PropertiesManager {

  val properties = new Properties()
  val filePath = System.getProperty("user.dir")
  val path: String = filePath + File.separator + "conf" + File.separator + "commerce.properties"
//  val path: String = "D:\\Code\\count_records\\src\\main\\resources\\commerce.properties"
  properties.load(new FileInputStream(path))


  def getValue(paramter: String): String ={
    properties.getProperty(paramter)
  }

}
