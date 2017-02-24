package com.iflytek.vine.statistics

import com.iflytek.avro.reflect.ReflectDataEx
import com.iflytek.avro.util.HadoopFSUtil
import com.iflytek.vine.usermodel.UserModel
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.avro.AvroReadSupport
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.JavaConversions._

/**
 * Created by Linxiao Bai on 2016/7/22.
 */

object InterestAnalysis{

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
      .cache()


    /*PV analysis here */
    val pv=modelData
      .map(x=>x._2.interest.map(x=>(x.value,1)))
      .flatMap(x=>x)
      .reduceByKey((x,y)=>x+y,1)

    val uv= modelData
      .map(x=>x._2.interest.map(x=>x.value).toSet.toArray.map(x=>(x,1)))
      .flatMap(x=>x)
      .reduceByKey((x,y)=>x+y,1)

    if(fs.exists(new Path(args(1)))) fs.delete(new Path(args(1)),true)
    if(fs.exists(new Path(args(2)))) fs.delete(new Path(args(2)),true)
    pv.saveAsTextFile(args(1))
    uv.saveAsTextFile(args(2))
  }
}
