package com.weather.bigdata.it.spark

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.weather.bigdata.it.utils.hdfsUtil.{HDFSFile, HDFSOperation1, HDFSReadWriteUtil}

import scala.collection.mutable.ListBuffer
import scala.util.control._

object monitorHDFS {
  private val basePath: String = PropertiesUtil.basePath
  private val eles: Array[String] = PropertiesUtil.eles
  private val baseMd5Path = PropertiesUtil.baseMd5Path
  private val breakTime: Long = PropertiesUtil.breakTime

  private val sdf = new SimpleDateFormat("yyyyMMdd")
  private val list = new util.ArrayList[String]
  private val DateS = sdf.format(new Date)



  def getMD5File(splitFile:String): String ={
    val md5Path=baseMd5Path +"/"+PropertiesUtil.getRegion(splitFile)
    if(!HDFSOperation1.exists(md5Path)){
      HDFSOperation1.mkdirs(md5Path)
    }
    val md5File =md5Path+ "/"+ DateS + ".dat"
    md5File
  }
  def main(args:Array[String]): Unit ={
    val splitFile=args(0)
    val md5File=this.getMD5File(splitFile)

    println(md5File)
    if(HDFSOperation1.exists(md5File)){
      val lineTxTs:Array[String]=HDFSReadWriteUtil.readTXT(md5File)
      lineTxTs.foreach(lineTxT=>{
        list.add(lineTxT.split(",").head)
      })
    }

    this.HDFSLister(splitFile)
  }
  def HDFSLister(splitFile:String): Unit ={
    val now = System.currentTimeMillis
    //    val day = sdf.format(date)
    val md5Path=this.getMD5File(splitFile)



    val outer :Breaks= new Breaks
    val inner :Breaks= new Breaks

    outer.breakable(
      eles.foreach(ele=>{
        val paths:String = basePath +"/"+ ele + "/" + DateS + "/"
        if(!HDFSOperation1.exists(paths)){
          PropertiesUtil.log.info("mkdir:"+paths)
          HDFSOperation1.mkdirs(paths)
        }else{
          PropertiesUtil.log.info("monitor:"+paths)
          val filenames:ListBuffer[String]=HDFSOperation1.listChildrenAbsoluteFile(paths)
          inner.breakable(
            if(filenames.size>0){
              filenames.foreach(filename=>{
                val md5:String=HDFSFile.fileMD5(filename)
                val mtime =HDFSFile.fileModificationTime(filename)
                if(!list.contains(md5) & (now - mtime <= breakTime*1000)){
                  val md5_kv = md5 + "," + filename
                  val flag0=HDFSReadWriteUtil.writeTXT(md5Path,md5_kv,true)
                  if(flag0){
                    PropertiesUtil.log.info("write MD5,md5_kv="+md5_kv+"(md5Path="+md5Path+")")
                    val signalMsgs=HDFSReadWriteUtil.readTXT(filename)
                    signalMsgs.foreach(signalMsg=>{
                      sparksubmit.jobsubmit_byMsg(signalMsg,splitFile,Array(signalMsg,splitFile))
                    })
                    outer.break()
                  }
                }
              })
            }
          )
        }
      })
    )

  }
}
