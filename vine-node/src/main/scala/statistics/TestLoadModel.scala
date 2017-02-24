package com.iflytek.vine.statistics

import com.iflytek.avro.reflect.ReflectDataEx
import com.iflytek.vine.usermodel.UserModel
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.avro.AvroReadSupport
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Linxiao Bai on 2016/7/21.
 */


object TestLoadModel{
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GetIPCount")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").setMaster("local")
    val sc = new SparkContext(conf)
    println("parquet 1.8.1")


    val amInputJob = Job.getInstance(sc.hadoopConfiguration)
    amInputJob.getConfiguration.set("parquet.avro.data.supplier", "org.apache.parquet.avro.ReflectDataSupplier")
    amInputJob.getConfiguration.set("parquet.avro.compatible", "false")
    ParquetInputFormat.setReadSupportClass(amInputJob, classOf[AvroReadSupport[UserModel]])
    val amSchema = ReflectDataEx.get().getSchema(classOf[UserModel])
    AvroReadSupport.setAvroReadSchema(amInputJob.getConfiguration, amSchema)


    val modelData = sc.newAPIHadoopFile(args(0), classOf[ParquetInputFormat[UserModel]], classOf[Void], classOf[UserModel], amInputJob.getConfiguration)

    modelData.map(x=>x._2.uid).saveAsTextFile(args(1))

  }
}
