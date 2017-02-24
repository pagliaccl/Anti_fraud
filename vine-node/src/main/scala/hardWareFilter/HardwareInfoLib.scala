package com.iflytek.vine.hardWareFilter

import com.iflytek.vine.tools.ShareSparkMethod
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path


/**
 * Created by Linxiao Bai on 2016/7/28.
  * Creates legal hardware library
 */
object HardwareInfoLib{
  def main(args: Array[String]) {
    val sc= ShareSparkMethod.getSparkContext("HardwareInforLib")

//    val sc=ShareSparkMethod.getSparkContextLocal("HardwareInfoLib")
    val fs = org.apache.hadoop.fs.FileSystem.get(new Configuration())

    val infile1=ShareSparkMethod.readExtractPair(args(0),sc)
      .map{x=>
        ((x._2.fields.data.get("manufact").toString,x._2.fields.data.get("dvc_model").toString,x._2.fields.data.get("resolution").toString),(x._2.fields.data.get("dvc").toString))
      }
      .distinct(100)
      .cache()

    val hardware_UV=infile1
      .map(x=>(x._1,1))
      .reduceByKey((x,y)=>x+y,1)
      .filter(x=>x._2>1)

    val user_hardwareNum=infile1
      .map(x=>(x._2,1))
      .reduceByKey((x,y)=>x+y,100)
      .cache()


    if(fs.exists(new Path(args(1)))) fs.delete(new Path(args(1)),true)
    hardware_UV.saveAsTextFile(args(1))
    if(fs.exists(new Path(args(2)))) fs.delete(new Path(args(2)),true)
    user_hardwareNum.saveAsTextFile(args(2))

    val hardwareNum_UV=user_hardwareNum
      .map(x=>(x._2,1))
      .reduceByKey((x,y)=>x+y,1)
      .sortBy(x=>x._2)

    if(fs.exists(new Path(args(3)))) fs.delete(new Path(args(3)),true)
    hardwareNum_UV.saveAsTextFile(args(3))


  }
}
