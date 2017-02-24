package com.iflytek.vine.tools

import com.iflytek.avro.mapreduce.input.AvroPairInputFormat
import com.iflytek.generic.model.VCOnlineAnalysisModel
import com.iflytek.hadoop.model.ExtractPair
import com.yooshu.tiny.Analysis.bean.YooshuModel
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Linxiao Bai on 2016/3/23.
  * This is a helper object that generates RDD, and other class entrances.
 */
object ShareSparkMethod {


  def getSparkContext(name:String) : SparkContext = {
    val conf = new SparkConf().setAppName(name)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.akka.frameSize", "512")
      //      .set("spark.worker.timeout","30000")
      .set("spark.akka.timeout","800s")
      .set("spark.network.timeout","600s")
      .set("spark.storage.blockManagerHeartBeatMs", "300000")

    new SparkContext(conf)
  }

  def getSparkContextLocal(name:String) : SparkContext = {
    val conf = new SparkConf().setAppName(name)
      .setMaster("local")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    new SparkContext(conf)
  }



  def readExtractPair(datasrc:String,sc:SparkContext): RDD[(String,ExtractPair)]= {
    sc.newAPIHadoopFile(datasrc,
      classOf[AvroPairInputFormat[String, ExtractPair]],
      classOf[String],
      classOf[ExtractPair],
      sc.hadoopConfiguration)
  }


  def readYooshuModel(datasrc:String,sc:SparkContext): RDD[(String,YooshuModel)]= {
    sc.newAPIHadoopFile(datasrc,
      classOf[AvroPairInputFormat[String, YooshuModel]],
      classOf[String],
      classOf[YooshuModel],
      sc.hadoopConfiguration)
  }



  def readVCOnlineAnalysisModel(datasrc:String,sc:SparkContext): RDD[(String,VCOnlineAnalysisModel)]= {
    sc.newAPIHadoopFile(datasrc,
      classOf[AvroPairInputFormat[String, VCOnlineAnalysisModel]],
      classOf[String],
      classOf[VCOnlineAnalysisModel],
      sc.hadoopConfiguration)
  }

}
