package com.iflytek.vine.hardWareFilter

import com.iflytek.vine.tools.ShareSparkMethod
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path



/**
 * Created by Linxiao Bai on 2016/7/28.
  * This object analyze legal hardware information based on third party data.
 */
object NewHardwareInfoLib{
  def main(args: Array[String]) {
    val sc= ShareSparkMethod.getSparkContext("newHardwareInfo")

    //    val sc=ShareSparkMethod.getSparkContextLocal("HardwareInfoLib")
    val fs = org.apache.hadoop.fs.FileSystem.get(new Configuration())

    val infile1=ShareSparkMethod.readVCOnlineAnalysisModel(args(0),sc)
      .map{x=>
        ((x._2.nonBasicFields.getData.get("manufact"),x._2.osModel,x._2.osResolution)
          ,(x._2.nonBasicFields.getData.get("imei"),x._2.nonBasicFields.getData.get("android_id"),x._2.nonBasicFields.getData.get("mac")
            ,x._2.nonBasicFields.getData.get("openudid")))
      }
      .filter(x=>x._1._1==null&& x._1._2==null)
      .distinct(100)
      .cache()
    println(infile1.count())

    val infile2=infile1
      .map{x=>
        if(x._2._1!=null && x._2._1.toString.length>=15)
          ((x._1._1,x._1._2,x._1._3,x._2._1.toString.substring(0,5)),1)
        else
          ((x._1._1,x._1._2,x._1._3,"unknow"),1)
      }
      .map { x =>
        var x1 = ""
        var x2 = ""
        var x3 = ""

        if (x._1._1 == null) x1 = "empty"
        else x1 = x._1._1.toString
        if (x._1._2 == null) x2 = "empty"
        else x2 = x._1._2.toString
        if (x._1._3 == null) x3 = "empty"
        else x3 = x._1._3.toString

        ((x1, x2, x3,x._1._4), x._2)
      }
      .reduceByKey(_ + _,100)
      .filter(x=>x._2>1)
      .distinct(1)
      .sortBy(x=>x._2,false)


//
//    val hardware_UV=infile1
//      .map(x=>(x._1,1))
//      .reduceByKey((x,y)=>x+y,1)
//      .filter(x=>x._2>1)
//
//    val user_hardwareNum=infile1
//      .map(x=>(x._2,1))
//      .reduceByKey((x,y)=>x+y,100)
//      .cache()
//

//    val hardwareNum_UV=user_hardwareNum
//      .map(x=>(x._2,1))
//      .reduceByKey((x,y)=>x+y,1)
//      .sortBy(x=>x._2)
//
//    if(fs.exists(new Path(args(3)))) fs.delete(new Path(args(3)),true)
//    hardwareNum_UV.saveAsTextFile(args(3))

    if(fs.exists(new Path(args(2)))) fs.delete(new Path(args(2)),true)
    infile2.saveAsTextFile(args(2))


  }
}
