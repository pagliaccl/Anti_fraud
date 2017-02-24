package com.iflytek.vine.statistics

import com.iflytek.vine.tools.ShareSparkMethod
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import scala.collection.JavaConversions._
/**
 * Created by Linxiao Bai on 2016/7/29.
 */
object IPTimeAnalysis{
  def main(args: Array[String]) {
    val sc= ShareSparkMethod.getSparkContext("IPTimeAnalysis")
    val fs = org.apache.hadoop.fs.FileSystem.get(new Configuration())

    val time=new SimpleDateFormat("HH")
    val ip=args(2)
//"106.2.230"
//"183.240.197"
//    val time=new SimpleDateFormat("HH")
    val data =ShareSparkMethod.readYooshuModel(args(0),sc)
//      .map{x=>
//        x._2.orders
//          .map(x=>(x._2.viewip,x._2.viewts*1000L)).toIterable
//          .filter(x=>x._1!=null && x._1.split('.').length==4)
//          .filter(x=>x._1.contains(ip))
//      }
//      .flatMap(x=>x.map(x=>(time.format(new Date(x._2)),1)))
      .map(x=>(x._2.ip,time.format(x._2.timestamp*1000L)))
      .filter(x=>x._1!=null && x._1.split('.').length==4)
      .filter(x=>x._1.contains(ip))
      .map(x=>(x._2,1))
      .reduceByKey((x,y)=>x+y,1)
      .sortBy(x=>x._1)
      .map(x=>x._1+"|"+x._2)
////      .flatMap(x=>x.map(x=>(time.format(new Date(x._2.toLong)),1)))
////      .reduceByKey((x,y)=>x+y,1)
//
    if(fs.exists(new Path(args(1)))) fs.delete(new Path(args(1)),true)
    data.saveAsTextFile(args(1))





  }
}
