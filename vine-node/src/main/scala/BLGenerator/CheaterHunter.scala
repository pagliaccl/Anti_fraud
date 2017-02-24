package com.iflytek.vine.BLGenerator

import com.iflytek.avro.reflect.ReflectDataEx
import com.iflytek.vine.model.Cheater
import com.iflytek.vine.trial.Trial
import com.iflytek.vine.usermodel.UserModel
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Linxiao Bai on 2016/7/20.
 */
object CheaterHunter {
  def main (args: Array[String]){
    val conf = new SparkConf().setAppName("CheaterHunter")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val date = args(0)
    val usermodelPath = "hdfs://ns-bj/user/junliu6/anticheat-job/yooshu/usermodel/"
    val trialAlgs = args(1)
    val trialAlgClassPath = "com.iflytek.vine.trial."
    val outputPath = "hdfs://ns-bj/user/junliu6/anticheat-job/yooshu/cheater/"+date+"/"

    val userInputJob = Job.getInstance(sc.hadoopConfiguration)
    userInputJob.getConfiguration.set("parquet.avro.data.supplier", "org.apache.parquet.avro.ReflectDataSupplier")
    userInputJob.getConfiguration.set("parquet.avro.compatible", "false")
    ParquetInputFormat.setReadSupportClass(userInputJob, classOf[AvroReadSupport[UserModel]])
    val usermodelSchema = ReflectDataEx.get().getSchema(classOf[UserModel])
    AvroReadSupport.setAvroReadSchema(userInputJob.getConfiguration, usermodelSchema)

    val cheaterOutputJob = Job.getInstance(sc.hadoopConfiguration)
    cheaterOutputJob.getConfiguration.set("parquet.avro.write.data.supplier", "org.apache.parquet.avro.ReflectDataSupplier")
    cheaterOutputJob.getConfiguration.set("parquet.compression", "snappy")
    cheaterOutputJob.getConfiguration.set("parquet.avro.write-old-list-structure", "false")
    ParquetOutputFormat.setWriteSupportClass(cheaterOutputJob, classOf[AvroWriteSupport[Cheater]])
    AvroParquetOutputFormat.setSchema(cheaterOutputJob, ReflectDataEx.get().getSchema(classOf[Cheater]))

    val cheaterInputJob = Job.getInstance(sc.hadoopConfiguration)
    cheaterInputJob.getConfiguration.set("parquet.avro.data.supplier", "org.apache.parquet.avro.ReflectDataSupplier")
    cheaterInputJob.getConfiguration.set("parquet.avro.compatible", "false")
    ParquetInputFormat.setReadSupportClass(cheaterInputJob, classOf[AvroReadSupport[Cheater]])
    val cheaterSchema = ReflectDataEx.get().getSchema(classOf[Cheater])
    AvroReadSupport.setAvroReadSchema(cheaterInputJob.getConfiguration, cheaterSchema)

    val usermodel = sc.newAPIHadoopFile(usermodelPath+date, classOf[ParquetInputFormat[UserModel]], classOf[Void], classOf[UserModel], userInputJob.getConfiguration).cache()

    for (trialAlg <- trialAlgs.split(",")) {
      val trial = trialAlgClassPath + trialAlg
      val clazz = Class.forName(trial).newInstance().asInstanceOf[Trial]
      clazz.trail(sc , usermodel , date , outputPath , cheaterOutputJob)
    }

    usermodel.unpersist()

//    summary()

    sc.stop()

    def summary(): Unit ={
      var cheater:RDD[(Void, Cheater)] = sc.parallelize(List((null,null)))
      for (trialAlg <- trialAlgs){
        cheater = cheater.union(sc.newAPIHadoopFile(outputPath+trialAlg, classOf[ParquetInputFormat[Cheater]], classOf[Void], classOf[Cheater], userInputJob.getConfiguration).cache())
      }
      cheater.filter(null!=_._2).distinct()
        .saveAsNewAPIHadoopFile(outputPath + "total", classOf[Void],
          classOf[Cheater], classOf[ParquetOutputFormat[Cheater]], cheaterOutputJob.getConfiguration)
    }

  }
}
