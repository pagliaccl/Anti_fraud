package com.iflytek.vine.statistics

import com.iflytek.vine.tools.ShareSparkMethod
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import scala.collection.JavaConversions._
/**
 * Created by Linxiao Bai on 2016/7/26.
  * This object monitors the behavior identified by IPv4 network segment
 */

object MonitorIPAnalysis{
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
      .cache()


    if(fs.exists(new Path(args(2)))) fs.delete(new Path(args(2)),true)
    data.sortBy(x=>x._2,false,1).saveAsTextFile(args(2))


    val data2=data
      .map(x=>(x._2,1))
      .reduceByKey((x,y)=>x+y,1)
      .sortBy(x=>x._1,false)

    if(fs.exists(new Path(args(1)))) fs.delete(new Path(args(1)),true)
    data2.saveAsTextFile(args(1))



  }
}
