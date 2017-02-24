package com.iflytek.vine.BLGenerator

import com.iflytek.vine.tools.ShareSparkMethod
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path


/**
 * Created by Linxiao Bai on 2016/8/10.
 */
object SuspiciousIp{
  def main(args: Array[String]) {
    val sc= ShareSparkMethod.getSparkContextLocal("SuspiciousIp")
    val fs = org.apache.hadoop.fs.FileSystem.get(new Configuration())

    val data= sc.textFile(args(0))
      .map{x=>
        val text=x.replace("(","").replace(")","").trim.split(',')
        if(!text(1).contains('.')){
          (text(0),text(1).toInt,text(3).toDouble)
        }
        else (text(0),text(2).toInt,
          text(4).toDouble)
      }
      .cache()

    /*Quantile Computation here*/
    val volume=data
      .map(x=>(x._2,1))
      .reduceByKey(_ + _)
      .collect()
      .sortBy(x=>x._1)


    val k= if(args.length>3 && args(2)!=null) args(2).toDouble else 3.0
    var size=0
    var q1=0.0
    var q3=0.0

    volume.map{x=>
      size+=x._2
    }

    var findq1=false
    var findq3=false
    var cursum=0
    volume.map{x=>
      if (findq1==false) {
        if (cursum >= 0.50 * size) {
          q1 = Math.log(x._1.toDouble)
          findq1 = true
        }
      }
      if(findq3==false){
        if(cursum>=0.95*size) {
          q3 = Math.log(x._1.toDouble)
          findq3 = true
        }
      }
      cursum+=x._2
    }


    val upperbound=q3+k*(q3-q1)
    println("maxV is ====>"+volume.last)
    println("Volume upperbound is ====>"+Math.exp(upperbound))

    /*Volume Selection here*/
    val suspicious2= data
      .filter(x=>Math.log(x._2)>upperbound)
      .sortBy(x=>x._3,false,1)
      .cache()

    println("Doing MSE selection in " + suspicious2.count()+" Candidates")


    /*compute score, quartile*/
    val mse=suspicious2
      .map(x=>(x._3,1))
      .reduceByKey(_ + _)
      .collect()
      .sortBy(x=>x._1)


    val k2= if(args.length>3 && args(3)!=null) args(3).toDouble else 1.5
    var size2=0
    var newq1=0.0
    var newq3=0.0

    var newfindq1=false
    var newfindq3=false
    var newcursum=0

    mse.map{x=>
      size2+=x._2
    }

    mse.map{x=>
      if (newfindq1==false) {
        if (newcursum >= 0.25 * size2) {
          newq1 = x._1.toDouble
          newfindq1 = true
        }
      }
      if(newfindq3==false){
        if(newcursum>=0.75*size2) {
          newq3 = x._1.toDouble
          newfindq3 = true
        }
      }
      newcursum+=x._2
    }


    val newupperbound=newq3+k2*(newq3-newq1)
    /*print score upper bound*/
    println("MSE upperbound is ====>"+ newupperbound)

    /*MSE Selection here*/
    val ipBlackList=suspicious2
      .filter(x=>x._3>newupperbound)
      .map(x=>x._1)
      .cache()

    println("Black List Size ====> " + ipBlackList.count())

    if(fs.exists(new Path(args(1)))) fs.delete(new Path(args(1)),true)
    ipBlackList.saveAsTextFile(args(1))


  }
}
