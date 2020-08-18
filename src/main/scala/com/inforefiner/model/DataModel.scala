package com.inforefiner.model


/**
  * 数据集路径和类型
  * @param id
  * @param storage
  * @param storage_configurations
  * @param record_number
  * @param name
  */
case class PathInformation(var id: String,
                            var storage: String,
                            var storage_configurations: String,
                            var record_number: Long,
                            var name: String)

/**
  * 采集数据
  * @param id
  * @param taskJson
  * @param writeOut
  * @param endTime
  */
case class CollectInformation(var id: String,
                               var taskJson: String,
                               var writeOut: String,
                               var endTime: String)


/**
  * 相同dataset 不同job
  * @param jobId
  * @param endTime
  */
case class CollectSameDataset(var jobId: String,
                               var endTime: String)
