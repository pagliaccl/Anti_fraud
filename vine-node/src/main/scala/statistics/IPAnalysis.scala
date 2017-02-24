package com.iflytek.vine.statistics

import com.iflytek.avro.reflect.ReflectDataEx
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

object IPAnalysis{

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

    val location=modelData
      .map(x=>x._2.location.map(x=>x.value.impIp).toArray)
      .map{x=>
        var clic=0
        var i= 1
        if(!x.isEmpty) {
          while (i < x.length) {
            if (x(i) != "" && x(i - 1) != "") {
              if (x(i - 1).split(".").length == 4 && x(i).split(".").length == 4) {
                var ip1 = x(i - 1).split(".")
                var ip2 = x(i).split(".")
                if (ip1(0) + ip1(1) + ip1(2) == ip2(0) + ip2(1) + ip2(2))
                  clic += 1
              }
            }
            i += 1
          }
        }
        (clic,1)
      }

    if(fs.exists(new Path(args(1)))) fs.delete(new Path(args(1)),true)
    location.reduceByKey((x,y)=>x+y,1).saveAsTextFile(args(1))
    }
}

