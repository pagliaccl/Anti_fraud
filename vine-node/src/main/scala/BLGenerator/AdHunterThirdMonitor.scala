package com.iflytek.vine.BLGenerator

import com.iflytek.avro.mapreduce.input.AvroPairInputFormat
import com.iflytek.avro.reflect.ReflectDataEx
import com.iflytek.gnome.data.analysis.model.adx.monitor.ThirdMonitorModel
import com.iflytek.maple.util.ReadPropertyUtil
import com.iflytek.vine.model.{AnalysisModel, EventId, UserIndex}
import com.iflytek.vine.report._
import com.iflytek.vine.util.{DbDriver, ModelConversion}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by Linxiao Bai on 2016/4/13.
  * This Object generate ad log report.
 */
object AdHunterThirdMonitor extends App {
  val conf = new SparkConf().setAppName("AdHunterThirdMonitor")
  val sc = new SparkContext(conf)

  println("parquet 1.8.1")
  val outputjob = Job.getInstance(sc.hadoopConfiguration)
  outputjob.getConfiguration.set("parquet.avro.write.data.supplier", "org.apache.parquet.avro.ReflectDataSupplier")
  outputjob.getConfiguration.set("parquet.compression", "snappy")
  outputjob.getConfiguration.set("parquet.avro.write-old-list-structure", "false")
  ParquetOutputFormat.setWriteSupportClass(outputjob, classOf[AvroWriteSupport[AnalysisModel]])
  AvroParquetOutputFormat.setSchema(outputjob, ReflectDataEx.get().getSchema(classOf[AnalysisModel]))

  val uiInputJob = Job.getInstance(sc.hadoopConfiguration)
  uiInputJob.getConfiguration.set("parquet.avro.data.supplier","org.apache.parquet.avro.ReflectDataSupplier")
  uiInputJob.getConfiguration.set("parquet.avro.compatible" , "false")
  ParquetInputFormat.setReadSupportClass(uiInputJob,classOf[AvroReadSupport[UserIndex]])
  val uiSchema = ReflectDataEx.get().getSchema(classOf[UserIndex])
  AvroReadSupport.setAvroReadSchema(uiInputJob.getConfiguration,uiSchema)

  val uiOutputJob = Job.getInstance(sc.hadoopConfiguration)
  uiOutputJob.getConfiguration.set("parquet.avro.write.data.supplier", "org.apache.parquet.avro.ReflectDataSupplier")
  uiOutputJob.getConfiguration.set("parquet.avro.write-old-list-structure", "false")
  uiOutputJob.getConfiguration.set("parquet.compression", "snappy")
  ParquetOutputFormat.setWriteSupportClass(uiOutputJob, classOf[AvroWriteSupport[UserIndex]])
  AvroParquetOutputFormat.setSchema(uiOutputJob, ReflectDataEx.get().getSchema(classOf[UserIndex]))

  val baseinputpath = "hdfs://ns-bj/user/dxzhang3/online/vc_log/extract/bj/ThirdMonitorModel/"
  val rootPath = "hdfs://ns-bj/user/"
  var reportPath = ""
  var parquetPath = ""
  val reportClassPath = "com.iflytek.vine.report.thirdmonitor."

  def prepare(sc:SparkContext , dataUserName:String): Unit ={
    reportPath = rootPath+dataUserName+"/anticheat-job/thirdmonitor/report_result/"
    parquetPath = rootPath+dataUserName+"/anticheat-job/thirdmonitor/model/"
    var path = new Path(reportPath)
    val fs = FileSystem.get(sc.hadoopConfiguration)
    if(!fs.exists(path))
      fs.mkdirs(path)

    path = new Path(parquetPath)
    if(!fs.exists(path))
      fs.mkdirs(path)
  }

  def cleanDate(sc:SparkContext , date:String , reports:String): Unit ={
    println("start clean data...")
    val fs = FileSystem.get(sc.hadoopConfiguration)

    var path = new Path(reportPath+date)
    if(fs.exists(path))
      fs.delete(path,true)

    path = new Path(parquetPath+date)
    if(fs.exists(path))
      fs.delete(path,true)

    try{
      val url = ReadPropertyUtil.getStringValue("dbconfig.pro","dburl","")
      Class.forName("com.mysql.jdbc.Driver")
      val conn = DriverManager.getConnection(url)
      var ps: PreparedStatement = null
      for(report <- reports.split(",")){
        val sql = "delete from "+report+" where registration_time='"+date+"' and plat_id='thirdmonitor';"
        ps = conn.prepareStatement(sql)
        ps.executeUpdate()
      }
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    } catch {
      case e: Exception => println("delete date exception!"+e)
    }
  }

  def report(sc:SparkContext , date:String ,  modelreports:String , countreports:String , isNewModel:Boolean): Unit ={
    val inputpath = baseinputpath+date+"/*/current/*"
    val outputpath = reportPath + date + "/"
    val modelPath = parquetPath + date + "/"

    println("inputpath:"+inputpath)
    println("outputpath:"+outputpath)
    println("modelPath:"+modelPath)

    val modelData = sc.newAPIHadoopFile(inputpath, classOf[AvroPairInputFormat[String, ThirdMonitorModel]],
      classOf[String], classOf[ThirdMonitorModel], sc.hadoopConfiguration)
      .map(
        x => ModelConversion.convertAdxModel2AdHunterModel(x._2)
      )
    .filter(x=>if (null!=x) true else false)
    modelData.persist(StorageLevel.MEMORY_AND_DISK_SER)

    modelData.map(model => (null, model)).saveAsNewAPIHadoopFile(modelPath, classOf[Void],
      classOf[AnalysisModel], classOf[ParquetOutputFormat[AnalysisModel]], outputjob.getConfiguration)

    for(modelreport<-modelreports.split(",")){
      val report = reportClassPath+modelreport
      println("report:"+report)
      val clazz = Class.forName(report).newInstance().asInstanceOf[ModelReport]
      println("class:"+clazz)
      println("start compute:"+System.currentTimeMillis())
      clazz.compute(modelData , date , outputpath)
      println("end compute:"+System.currentTimeMillis())
    }

    //用于用户数和pv量在各指标的统计
    val countData = modelData.map(
      x => {
        val calendar = Calendar.getInstance()
        calendar.setTimeInMillis(x.events.get(0).get(EventId.req).ts)

        val imps = ArrayBuffer[String]()
        val clis = ArrayBuffer[String]()
        x.events.foreach(
        eventMap=>{
          if (eventMap.containsKey(EventId.imp)) imps += eventMap.get(EventId.imp).ip
          if (eventMap.containsKey(EventId.cli)) clis += eventMap.get(EventId.cli).ip
        }
        )
        ((x.platId, x.mediaKey, x.deviceInfo.uid, x.deviceInfo.provice, x.deviceInfo.devModel, x.deviceInfo.ntt, x.adunitId, calendar.get(Calendar.HOUR_OF_DAY) , x.deviceInfo.ip , imps , clis , x.deviceInfo.operator , x.deviceInfo.userAgent , x.deviceInfo.osSystem , x.deviceInfo.osVersion , x.deviceInfo.imei , x.deviceInfo.osVersion , x.deviceInfo.vendor , x.deviceInfo.devResolution), (x.events.size() , x.events.count(_.contains(EventId.imp)) , x.events.count(_.contains(EventId.cli))))
      }
    ).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2,x._3+y._3), 80).persist(StorageLevel.MEMORY_ONLY_SER)

    var i = 0
    for(countreport<-countreports.split(",")){
      val report = reportClassPath+countreport
      println("report:"+report)
      val clazz = Class.forName(report).newInstance().asInstanceOf[CountReport]
      println("class:"+clazz)
      clazz.compute(countData , null , date , outputpath)
      println("start compute:"+System.currentTimeMillis())
      println("end compute:"+System.currentTimeMillis())
      if(i==0){
        modelData.unpersist()
      }
      i+=1
    }
    countData.unpersist()
  }

  def persist2DB(sc:SparkContext , date:String): Unit ={
    val inputpath = reportPath + date +"/*/*"
    val data = sc.newAPIHadoopFile(inputpath, classOf[AvroPairInputFormat[String, ReportObject]],
      classOf[String], classOf[ReportObject], sc.hadoopConfiguration)
      .foreachPartition(
      {
        DbDriver.insert
      }
      )
  }

  def createReportDoc(date:String , needMail:Boolean, platId: String): Unit ={
    val report = new DailyDocReport
    report.report(date, null, null, platId)
    if(needMail)
      report.reportMail(date)
  }

  def runJob(sc:SparkContext): Unit ={

    val startDate = args(0)
    println("startDate:"+startDate)

    val endDate = args(1)
    println("endDate:"+endDate)

    val dataUserName = args(2)
    println("user:"+dataUserName)

    val modelReportAlg = args(3)
    println("modelReportAlg:"+modelReportAlg)

    val countReportAlg = args(4)
    println("countReportAlg:"+countReportAlg)

    val isNewModel = args(5).toBoolean

    val needMail = args(6).toBoolean

    prepare(sc , dataUserName)

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val startCal = Calendar.getInstance()
    startCal.setTime(dateFormat.parse(startDate))
    val endCal = Calendar.getInstance()
    endCal.setTime(dateFormat.parse(endDate))

    while(startCal.before(endCal)){
      val date = dateFormat.format(startCal.getTime)
      cleanDate(sc , date , modelReportAlg+","+countReportAlg)

      report(sc , date , modelReportAlg , countReportAlg, isNewModel)

      persist2DB(sc,date)

      createReportDoc(date , needMail, args(7))

      startCal.add(Calendar.DATE , 1)
    }
  }

  runJob(sc)

  sc.stop()
}
