package com.iflytek.vine.statistics

import com.iflytek.vine.tools.ShareSparkMethod
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import scala.collection.JavaConversions._
/**
 * Created by Linxiao Bai on 2016/7/26.
 */

object GetTopIPInfo{
  def main(args: Array[String]) {
    val sc= ShareSparkMethod.getSparkContext("MonitorIPAnalysis")
    val fs = org.apache.hadoop.fs.FileSystem.get(new Configuration())

    val data= ShareSparkMethod.readYooshuModel(args(0),sc).repartition(100)
      .map{x=>
        x._2.orders.map(x=>x._2.viewip)
      }
      .flatMap(x=>x)
      .filter(x=>x!=null && x.split('.').length==4)
      .map {x =>
        val ip=x.split('.')
        (ip(0)+"."+ip(1)+"."+ip(2),1)
      }
      .reduceByKey((x,y)=>x+y)
      .filter(x=>x._2>180000)   //Filter IPs that PV greater than the threshold.
      .sortBy(x=>x._2,false,1)

    if(fs.exists(new Path(args(1)))) fs.delete(new Path(args(1)),true)
    data.saveAsTextFile(args(1))



  }
}
