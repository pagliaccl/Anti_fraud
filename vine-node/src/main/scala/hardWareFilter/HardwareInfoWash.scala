package com.iflytek.vine.hardWareFilter

import com.iflytek.vine.tools.ShareSparkMethod
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/**
 * Created by Linxiao Bai on 2016/8/5.
  * This object washes Null data.
 */
object HardwareInfoWash {
  def main(args: Array[String]) {
    val sc = ShareSparkMethod.getSparkContextLocal("HardwareInfoWash")
    val fs = org.apache.hadoop.fs.FileSystem.get(new Configuration())

    val data = sc.textFile(args(0))
      .map { x =>
        val splt = x.replace("(", "").replace(")", "").split(',')
        if (splt(0).contains("|")) {
          val temp = splt(0).split("|")
          (temp(0), temp(1), splt(2))
        }

        else if (splt(1).contains("|")) {
          val temp = splt(1).split("|")
          (temp(0), temp(1), splt(2))
        }

        else (splt(0),splt(1),splt(2))
      }
      .filter(x=> !(x._1+x._2+x._3).contains("empty"))
      .filter(x=> !(x._1+x._2+x._3).contains("unknow"))
      .filter{x=>
        x._1!=""&&
        x._2!=""&&
        x._3!=""
      }
      .filter{x=>
        x._1!="　　"&&
        x._2!="　　"&&
        x._3!="　　"
      }

    if(fs.exists(new Path(args(1)))) fs.delete(new Path(args(1)),true)
    data.saveAsTextFile(args(1))
  }
}
