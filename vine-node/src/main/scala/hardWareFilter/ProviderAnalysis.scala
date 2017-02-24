package com.iflytek.vine.hardWareFilter

import com.iflytek.avro.reflect.ReflectDataEx
import com.iflytek.vine.usermodel.UserModel
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Linxiao Bai on 2016/7/21.
  * This object does provider ananlysis on ad log.
 */


object ProviderAnalysis{
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GetIPCount")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val fs = org.apache.hadoop.fs.FileSystem.get(new Configuration())
    println("parquet 1.8.1")


    val amInputJob = Job.getInstance(sc.hadoopConfiguration)
    amInputJob.getConfiguration.set("parquet.avro.data.supplier", "org.apache.parquet.avro.ReflectDataSupplier")
    amInputJob.getConfiguration.set("parquet.avro.compatible", "false")
    ParquetInputFormat.setReadSupportClass(amInputJob, classOf[AvroReadSupport[UserModel]])
    val amSchema = ReflectDataEx.get().getSchema(classOf[UserModel])
    AvroReadSupport.setAvroReadSchema(amInputJob.getConfiguration, amSchema)

    val modelData = sc.newAPIHadoopFile(args(0), classOf[ParquetInputFormat[UserModel]], classOf[Void], classOf[UserModel], amInputJob.getConfiguration)
      .repartition(100)

    if(fs.exists(new Path(args(1)))) fs.delete(new Path(args(1)),true)
    modelData.map(x=>(x._2.operator_recently.toArray.toSet.size,1)).reduceByKey((x,y)=>x+y,1).saveAsTextFile(args(1))

  }
}
