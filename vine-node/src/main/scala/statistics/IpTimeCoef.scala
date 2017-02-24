package com.iflytek.vine.statistics

import com.iflytek.vine.tools.{HdfsFileRW, ShareSparkMethod, Similarity, SqrtError}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.expressions.Sqrt

import scala.collection.JavaConversions._


/**
 * Created by Linxiao Bai on 2016/7/29.
  * This object analyze UV timestamp to Hourly manner. Used for future statistics testing
 */
object IpTimeCoef{
  def main(args: Array[String]) {
    val sc= ShareSparkMethod.getSparkContext("IPTimeAnalysis")
    val fs = org.apache.hadoop.fs.FileSystem.get(new Configuration())

    val time=new SimpleDateFormat("HH")
    val conf_level=0.515
    val templateHashMap=new JavaHashMap[String,java.lang.Double]()
      templateHashMap.put("00",0D)
      templateHashMap.put("01",0D)
      templateHashMap.put("02",0D)
      templateHashMap.put("03",0D)
      templateHashMap.put("04",0D)
      templateHashMap.put("05",0D)
      templateHashMap.put("06",0D)
      templateHashMap.put("07",0D)
      templateHashMap.put("08",0D)
      templateHashMap.put("09",0D)
      templateHashMap.put("10",0D)
      templateHashMap.put("11",0D)
      templateHashMap.put("12",0D)
      templateHashMap.put("13",0D)
      templateHashMap.put("14",0D)
      templateHashMap.put("15",0D)
      templateHashMap.put("16",0D)
      templateHashMap.put("17",0D)
      templateHashMap.put("18",0D)
      templateHashMap.put("19",0D)
      templateHashMap.put("20",0D)
      templateHashMap.put("21",0D)
      templateHashMap.put("22",0D)
      templateHashMap.put("23",0D)

    /*loding Data here*/

    val data =ShareSparkMethod.readYooshuModel(args(0),sc)
      .map { x =>
        (x._2.ip, time.format(new Date(x._2.timestamp*1000L)))
        //        x._2.orders
        //          .map(x=>(x._2.viewip,x._2.viewts*1000L)).toIterable
        //          .filter(x=>x._1!=null && x._1.split('.').length==4)
        //          .map{x=>
        //            val ip=x._1.split('.')
        //            (x._1,x._2)
        ////            (ip(0)+"."+ip(1)+"."+ip(2),x._2)
        //          }
        //      }
        //      .flatMap(x=>x.map(x=>(x._1,time.format(new Date(x._2)))))
      }
      .cache()


    val popSeriMap=new JavaHashMap[String,java.lang.Double](templateHashMap)
    val populationTrend=data
      .map(x=>(x._2,1D))
      .reduceByKey((x,y)=>x+y,1)
      .collect
      .asInstanceOf[Array[(String,java.lang.Double)]]
      .map{x=>
        popSeriMap.put(x._1,x._2)
      }

    var allsize=0.0
    popSeriMap.map{x=>
      allsize+=x._2
    }
    popSeriMap.map{x=>
      popSeriMap.put(x._1,x._2/allsize)
    }


    /*UV statistics*/
    val data2=data
      .groupByKey(100) // shuffle to 100 partitions.
      .map{x=>
        val size=x._2.size
        val temp=new JavaHashMap[String,java.lang.Double](templateHashMap)
        x._2.map{y=>
          val newvalue=temp.get(y)
          temp.put(y,(newvalue+1D).asInstanceOf[java.lang.Double])
        }
        temp.map{x=>
          val newEle=x
          temp.put(x._1,x._2/size)
        }
        val coef=SqrtError.mse(temp,popSeriMap)
        val pass=coef>conf_level

        (x._1,size,true,coef)
      }
      .cache


    /*PV statistics*/
    val data3=data2
      .filter(x=>x._3==true)
      .repartition(1)
      .sortBy(x=>x._2,false)


    /*outPutting results*/

    if(fs.exists(new Path(args(1)))) fs.delete(new Path(args(1)),true)
    data2.saveAsTextFile(args(1))


    if(fs.exists(new Path(args(2)))) fs.delete(new Path(args(2)),true)
    data3.saveAsTextFile(args(2))


    if(fs.exists(new Path(args(3)))) fs.delete(new Path(args(3)),true)
    val hdfswriter=new HdfsFileRW().FileWrite(fs,args(3))
    popSeriMap.map{x=>
      hdfswriter.write(x._1+'|'+x._2.toString+"\n")
      hdfswriter.flush()
    }

    hdfswriter.close()

  }
}
