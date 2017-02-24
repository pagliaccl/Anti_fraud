package com.iflytek.vine.statistics

import com.iflytek.avro.reflect.ReflectDataEx
import com.iflytek.vine.model.{AnalysisModel, EventId}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by Linxiao Bai on 2016/7/19.
 */
object GetIPCount {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("GetIPCount")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    println("parquet 1.8.1")


//  This is a list of some suspicious IPs based on online observation
    val ipList = List("1.180.237.224", "123.150.202.84", "180.152.44.240", "223.99.163.248", "223.99.163.249", "223.99.163.250", "42.81.66.128", "42.81.66.130", "42.81.66.132", "58.216.100.13", "58.216.100.6", "61.164.212.238")

    val amInputJob = Job.getInstance(sc.hadoopConfiguration)
    amInputJob.getConfiguration.set("parquet.avro.data.supplier", "org.apache.parquet.avro.ReflectDataSupplier")
    amInputJob.getConfiguration.set("parquet.avro.compatible", "false")
    ParquetInputFormat.setReadSupportClass(amInputJob, classOf[AvroReadSupport[AnalysisModel]])
    val amSchema = ReflectDataEx.get().getSchema(classOf[AnalysisModel])
    AvroReadSupport.setAvroReadSchema(amInputJob.getConfiguration, amSchema)


    val modelData = sc.newAPIHadoopFile(args(0), classOf[ParquetInputFormat[AnalysisModel]], classOf[Void], classOf[AnalysisModel], amInputJob.getConfiguration).repartition(100)


//  Get those ip behaviors on media "56248f74"
    val data = modelData
      .filter(x => x._2.mediaKey == "56248f74" || x._2.mediaKey == "56249026")
      .map{case (key,value)=>
        val event=value.events
        event.map(x=>{
          if(x.keySet.contains(EventId.imp) && ipList.contains(x(EventId.imp).ip))   ((EventId.imp,x(EventId.imp).ip),1)
          else if (x.keySet.contains(EventId.cli) && ipList.contains(x(EventId.cli).ip)) ((EventId.cli,x(EventId.cli).ip),1)
          else (("Other","Other"),1)
          }
        )}
      .flatMap(x=>x)
      .reduceByKey((x,y)=>x+y,1)

    data.saveAsTextFile(args(1))
  }
}

