//package com.iflytek.vine.statistics
//
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.{SparkConf, SparkContext}
//
///**
// * Created by Linxiao Bai on 2016/4/28.
// */
//object SqlQuery {
//
//  def main(args: Array[String]) {
//    val conf = new SparkConf().setAppName("SqlQuery")
//      .setMaster("local[4]")
//      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .set("spark.local.dir", "/user/tmp")
//
//    val sc = new SparkContext(conf)
//    val sqlContext = new SQLContext(sc)
//
//    val inputDir = "/user/junliu6/anticheat-job/yooshu/usermodel/2016-07-18/"
//
//    val inputRdd = sqlContext.read.parquet(inputDir)
//
//    inputRdd.toDF().registerTempTable("AD_MODEL")
//
//    inputRdd.printSchema()
//
//    inputRdd.show(100)
//  }
//}
