package com.iflytek.vine.tools

/**
  * @author Linxiao Bai
  *         This is a helper function generate hdfs IO stream to IO master data to hdfs.
  */

import java.io.{BufferedReader, BufferedWriter, IOException, InputStreamReader, OutputStreamWriter}

import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileStatus, FileSystem, Path}

class HdfsFileRW {

  /*Reader*/
  @throws(classOf[IOException])
  def FileRead(fs: FileSystem, filename: String): BufferedReader = {
    val file: FileStatus = fs.getFileStatus(new Path(filename))
    val inputStream: FSDataInputStream = fs.open(file.getPath)
    val br = new BufferedReader(new InputStreamReader(inputStream))
    return br
  }


  /*Writer*/
  @throws(classOf[IOException])
  def FileWrite(fs: FileSystem, filename: String, append:Boolean=true): BufferedWriter = {
    val outputStream: FSDataOutputStream = fs.create(new Path(filename),append)
    val bw: BufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream))
    return bw
  }
}
