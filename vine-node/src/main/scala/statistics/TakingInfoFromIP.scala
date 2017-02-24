package com.iflytek.vine.statistics

import com.iflytek.vine.tools.ShareSparkMethod
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import scala.collection.JavaConversions._
/**
 * Created by Linxiao Bai on 2016/7/29.
 */
object TakingInfoFromIP{
  def main(args: Array[String]) {
    val sc= ShareSparkMethod.getSparkContext("IPTimeAnalysis")
    val fs = org.apache.hadoop.fs.FileSystem.get(new Configuration())

    val ip="183.240.197" // target IP example.
    //    val time=new SimpleDateFormat("HH")
    val data =ShareSparkMethod.readYooshuModel(args(0),sc)
      .map{x=>
        (x._2.uid, x._2.appid, x._2.adx, x._2.orders
          .map(x=>(x._2.viewip,1)).toIterable
          .filter(x=>x._1!=null && x._1.split('.').length==4)
          .count(x=>x._1.contains(ip))
          )
      }
      .filter(x=>x._4>0)
      .cache()

    val uidfreq=data.map(x=>(x._1,1))
      .reduceByKey((x,y)=>x+y,1)
      .sortBy(x=>x._2,false)

    val appfreq=data.map(x=>(x._2,1))
      .reduceByKey((x,y)=>x+y,1)
      .sortBy(x=>x._2,false)

    val adxfreq=data.map(x=>(x._3,1))
      .reduceByKey((x,y)=>x+y,1)
      .sortBy(x=>x._2,false)



    if(fs.exists(new Path(args(1)))) fs.delete(new Path(args(1)),true)
    uidfreq.saveAsTextFile(args(1))

    if(fs.exists(new Path(args(2)))) fs.delete(new Path(args(2)),true)
    appfreq.saveAsTextFile(args(2))

    if(fs.exists(new Path(args(3)))) fs.delete(new Path(args(3)),true)
    adxfreq.saveAsTextFile(args(3))

  }
}
