package com.iflytek.vine.BLGenerator

import com.iflytek.avro.mapreduce.input.AvroPairInputFormat
import com.iflytek.avro.reflect.ReflectDataEx
import com.iflytek.maple.util.ReadPropertyUtil
import com.iflytek.vine.model.{AnalysisModel, EventId, UserIndex}
import com.iflytek.vine.report._
import com.iflytek.vine.util.DbDriver
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by Linxiao Bai on 2016/4/13.
 */
object AdHunter extends App {
  val conf = new SparkConf().setAppName("AdHunter")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  val sc = new SparkContext(conf)

  println("parquet 1.8.1")

  val amInputJob = Job.getInstance(sc.hadoopConfiguration)
  amInputJob.getConfiguration.set("parquet.avro.data.supplier", "org.apache.parquet.avro.ReflectDataSupplier")
  amInputJob.getConfiguration.set("parquet.avro.compatible", "false")
  ParquetInputFormat.setReadSupportClass(amInputJob, classOf[AvroReadSupport[AnalysisModel]])
  val amSchema = ReflectDataEx.get().getSchema(classOf[AnalysisModel])
  AvroReadSupport.setAvroReadSchema(amInputJob.getConfiguration, amSchema)

  val amOutputJob = Job.getInstance(sc.hadoopConfiguration)
  amOutputJob.getConfiguration.set("parquet.avro.write.data.supplier", "org.apache.parquet.avro.ReflectDataSupplier")
  amOutputJob.getConfiguration.set("parquet.compression", "snappy")
  amOutputJob.getConfiguration.set("parquet.avro.write-old-list-structure", "false")
  ParquetOutputFormat.setWriteSupportClass(amOutputJob, classOf[AvroWriteSupport[AnalysisModel]])
  AvroParquetOutputFormat.setSchema(amOutputJob, ReflectDataEx.get().getSchema(classOf[AnalysisModel]))

  val uiInputJob = Job.getInstance(sc.hadoopConfiguration)
  uiInputJob.getConfiguration.set("parquet.avro.data.supplier", "org.apache.parquet.avro.ReflectDataSupplier")
  uiInputJob.getConfiguration.set("parquet.avro.compatible", "false")
  ParquetInputFormat.setReadSupportClass(uiInputJob, classOf[AvroReadSupport[UserIndex]])
  val uiSchema = ReflectDataEx.get().getSchema(classOf[UserIndex])
  AvroReadSupport.setAvroReadSchema(uiInputJob.getConfiguration, uiSchema)

  val uiOutputJob = Job.getInstance(sc.hadoopConfiguration)
  uiOutputJob.getConfiguration.set("parquet.avro.write.data.supplier", "org.apache.parquet.avro.ReflectDataSupplier")
  uiOutputJob.getConfiguration.set("parquet.avro.write-old-list-structure", "false")
  uiOutputJob.getConfiguration.set("parquet.compression", "snappy")
  ParquetOutputFormat.setWriteSupportClass(uiOutputJob, classOf[AvroWriteSupport[UserIndex]])
  AvroParquetOutputFormat.setSchema(uiOutputJob, ReflectDataEx.get().getSchema(classOf[UserIndex]))

  val rootPath = "hdfs://ns-bj/user/"
  var reportPath = ""
  var parquetPath = ""
  var userbitmapPath = ""
  var todaynewuserPath = ""
  val rootReportClassPath = "com.iflytek.vine.report."

  def prepare(sc: SparkContext, dataUserName: String , platId:String): Unit = {
    if("yooshu".equals(platId)){
      reportPath = rootPath + dataUserName + "/anticheat-job/yooshu/report_result/"
      parquetPath = rootPath + dataUserName + "/anticheat-job/yooshu/model/"
      userbitmapPath = rootPath + dataUserName + "/anticheat-job/yooshu/userbitmap/"
    }
    else {
      reportPath = rootPath + dataUserName + "/anticheat-job/report_result/"
      parquetPath = rootPath + dataUserName + "/anticheat-job/model/"
      userbitmapPath = rootPath + dataUserName + "/anticheat-job/userbitmap/"
    }
    todaynewuserPath = userbitmapPath + "index/newuser/"

    var path = new Path(reportPath)
    val fs = FileSystem.get(sc.hadoopConfiguration)
    if (!fs.exists(path))
      fs.mkdirs(path)

    path = new Path(parquetPath)
    if (!fs.exists(path))
      fs.mkdirs(path)
  }

  def cleanReports(reports: String, date: String , platId:String): Unit = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    var path = new Path(reportPath + date)

    val url = ReadPropertyUtil.getStringValue("dbconfig.pro", "dburl", "")
    Class.forName("com.mysql.jdbc.Driver")
    val conn = DriverManager.getConnection(url)
    var ps: PreparedStatement = null
    var sql = ""

    for (report <- reports.split(",")) {
      report match {
        case "AD_IP_PVRANGE" => {
          path = new Path(reportPath + date + "/" + report + "_REQ")
          if (fs.exists(path))
            fs.delete(path, true)

          path = new Path(reportPath + date + "/" + report + "_IMP")
          if (fs.exists(path))
            fs.delete(path, true)

          path = new Path(reportPath + date + "/" + report + "_CLI")
          if (fs.exists(path))
            fs.delete(path, true)
        }
        case "AD_USER_PVRANGE" => {
          path = new Path(reportPath + date + "/" + report + "_REQ")
          if (fs.exists(path))
            fs.delete(path, true)

          path = new Path(reportPath + date + "/" + report + "_IMP")
          if (fs.exists(path))
            fs.delete(path, true)

          path = new Path(reportPath + date + "/" + report + "_CLI")
          if (fs.exists(path))
            fs.delete(path, true)
        }
        case _ => {
          path = new Path(reportPath + date + "/" + report)
          if (fs.exists(path))
            fs.delete(path, true)
        }
      }
      sql = "delete from " + report + " where registration_time='" + date + "' and plat_id='+"+platId+"';"
      ps = conn.prepareStatement(sql)
      ps.executeUpdate()
    }

    if (ps != null) {
      ps.close()
    }
    if (conn != null) {
      conn.close()
    }
  }

  def cleanDate(sc: SparkContext, date: String, reportAlgs: String , platId:String): Unit = {
    println("start clean report data...")
    cleanReports(reportAlgs , date , platId)
  }

  def report(sc: SparkContext, date: String, reports: String): Unit = {
    val outputpath = reportPath + date + "/"
    val modelPath = parquetPath + date + "/"

    val modelreports = new ArrayBuffer[String]()
    val countreports = new ArrayBuffer[String]()
    val bitmapreports = new ArrayBuffer[String]()

    var reportClassPath = ""
    for (report <- reports.split(",")) {
      reportClassPath = rootReportClassPath + report
      Class.forName(reportClassPath).newInstance() match {
        case _: ModelReport => modelreports += report
        case _: CountReport => countreports += report
        case _: BitmapReport => bitmapreports += report
        case _ =>
      }
    }

    val modelData = sc.newAPIHadoopFile(modelPath, classOf[ParquetInputFormat[AnalysisModel]], classOf[Void], classOf[AnalysisModel], amInputJob.getConfiguration)
        .map(_._2)
//    modelData.persist(StorageLevel.MEMORY_AND_DISK_SER)

    for (modelreport <- modelreports) {
      val report = rootReportClassPath + modelreport
      val clazz = Class.forName(report).newInstance().asInstanceOf[ModelReport]
      clazz.compute(modelData, date, outputpath)
    }

    //Statistics on UV, PV
    val countData = modelData.map(
      x => {
        val calendar = Calendar.getInstance()
        calendar.setTimeInMillis(x.events.get(0).get(EventId.req).ts)

        val imps = ArrayBuffer[String]()
        val clis = ArrayBuffer[String]()
        x.events.foreach(
          eventMap => {
            if (eventMap.containsKey(EventId.imp)) imps += eventMap.get(EventId.imp).ip
            if (eventMap.containsKey(EventId.cli)) clis += eventMap.get(EventId.cli).ip
          }
        )
        ((x.platId, x.mediaKey, x.deviceInfo.uid, x.deviceInfo.provice, x.deviceInfo.devModel, x.deviceInfo.ntt, x.adunitId, calendar.get(Calendar.HOUR_OF_DAY), x.deviceInfo.ip, imps, clis, x.deviceInfo.operator, x.deviceInfo.userAgent, x.deviceInfo.osSystem, x.deviceInfo.osVersion, x.deviceInfo.imei, x.deviceInfo.osVersion, x.deviceInfo.vendor, x.deviceInfo.devResolution), (x.events.size(), x.events.count(_.contains(EventId.imp)), x.events.count(_.contains(EventId.cli))))
      }
    ).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3))
      .cache()

    val newUserIndex = sc.newAPIHadoopFile(todaynewuserPath + date, classOf[ParquetInputFormat[UserIndex]], classOf[Void], classOf[UserIndex], uiInputJob.getConfiguration)
      .map(x => ((x._2.mediaKey, x._2.uid), x._2.index))
      .cache()

    for (countreport <- countreports) {
      val report = rootReportClassPath + countreport
      println("report:" + report)
      val clazz = Class.forName(report).newInstance().asInstanceOf[CountReport]
      println("class:" + clazz)
      clazz.compute(countData, newUserIndex, date, outputpath)
      println("start compute:" + System.currentTimeMillis())
      println("end compute:" + System.currentTimeMillis())
    }

//    modelData.unpersist()
    countData.unpersist()
    newUserIndex.unpersist()

    for (bitmapreport <- bitmapreports) {
      val report = rootReportClassPath + bitmapreport
      println("report:" + report)
      val clazz = Class.forName(report).newInstance().asInstanceOf[BitmapReport]
      println("class:" + clazz)
      println("start compute:" + System.currentTimeMillis())
      clazz.compute(sc, date, userbitmapPath , outputpath)
      println("end compute:" + System.currentTimeMillis())
    }
  }

  def persist2DB(sc: SparkContext, date: String, reportAlgs: String): Unit = {
    var inputpath = reportPath + date
    for (report <- reportAlgs.split(",")) {
      report match {
        case "AD_IP_PVRANGE" => {
          inputpath = reportPath + date + "/" + report + "_REQ"
          save(sc, inputpath)

          inputpath = reportPath + date + "/" + report + "_IMP"
          save(sc, inputpath)

          inputpath = reportPath + date + "/" + report + "_CLI"
          save(sc, inputpath)
        }
        case "AD_USER_PVRANGE" => {
          inputpath = reportPath + date + "/" + report + "_REQ"
          save(sc, inputpath)

          inputpath = reportPath + date + "/" + report + "_IMP"
          save(sc, inputpath)

          inputpath = reportPath + date + "/" + report + "_CLI"
          save(sc, inputpath)

        }
        case _ => {
          inputpath = reportPath + date + "/" + report
          save(sc, inputpath)
        }
      }
    }

    def save(sc: SparkContext, inputpath: String): Unit = {
      sc.newAPIHadoopFile(inputpath, classOf[AvroPairInputFormat[String, ReportObject]],
        classOf[String], classOf[ReportObject], sc.hadoopConfiguration)
        .repartition(80)
        .foreachPartition(
        {
          DbDriver.insert
        }
        )
    }
  }

  def createReportDoc(date: String, needMail: Boolean, platId: String): Unit = {
    val report = new DailyDocReport
    report.report(date, null, null, platId)
    if (needMail)
      report.reportMail(date)
  }

  def runJob(sc: SparkContext): Unit = {

    val startDate = args(0)
    println("startDate:" + startDate)

    val endDate = args(1)
    println("endDate:" + endDate)

    val dataUserName = args(2)
    println("user:" + dataUserName)

    val reportAlgs = args(3)
    println("reportAlgs:" + reportAlgs)

    val platId = args(4)

    val needMail = args(5).toBoolean

    prepare(sc, dataUserName , platId)

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val startCal = Calendar.getInstance()
    startCal.setTime(dateFormat.parse(startDate))
    val endCal = Calendar.getInstance()
    endCal.setTime(dateFormat.parse(endDate))

    while (startCal.before(endCal)) {
      val date = dateFormat.format(startCal.getTime)

      cleanDate(sc, date, reportAlgs , platId)

      report(sc, date, reportAlgs)

      persist2DB(sc, date, reportAlgs)

      createReportDoc(date, needMail, platId)

      startCal.add(Calendar.DATE, 1)
    }
  }

  runJob(sc)

  sc.stop()
}
