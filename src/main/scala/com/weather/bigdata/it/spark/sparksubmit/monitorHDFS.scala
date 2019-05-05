package com.weather.bigdata.it.spark.sparksubmit

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.weather.bigdata.it.spark.platform.signal.JsonStream
import com.weather.bigdata.it.spark.platform.split.Analysis
import com.weather.bigdata.it.spark.sparksubmit.platformOperation.platformRole
import com.weather.bigdata.it.utils.hdfsUtil.{HDFSFile, HDFSOperation1, HDFSReadWriteUtil}
import org.apache.log4j.MDC

import scala.collection.mutable.ListBuffer
import scala.util.control._

object monitorHDFS {
  private val md5split=","

  private var basePath0: String = null
  private var eles0: Array[String] = null
  private var baseMd5Path0:String = null
  private var breakTime0: Long = Long.MaxValue
  private def getbasePath(splitFile: String): String = {
    if(this.basePath0 == null){
      this.basePath0=sparksubmit_PropertiesUtil.basePath(splitFile)
    }
    this.basePath0
  }
  private def geteles(splitFile: String): Array[String] = {
    if(this.eles0 == null){
      this.eles0=sparksubmit_PropertiesUtil.eles(splitFile)
    }
    this.eles0
  }

  private def getbaseMd5Path( splitFile: String):String = {
    if(this.baseMd5Path0==null){
      this.baseMd5Path0=sparksubmit_PropertiesUtil.baseMd5Path(splitFile)
    }
    this.baseMd5Path0
  }
  private def getmd5list(splitFile:String):util.ArrayList[String]={
    val md5list = new util.ArrayList[String]
    val md5File=this.getMD5RecordFile(splitFile)
    sparksubmit_PropertiesUtil.log_monitorHDFS.info(md5File)
    if(HDFSOperation1.exists(md5File)){
      val lineTxTs:Array[String]=HDFSReadWriteUtil.readTXT(md5File)
      lineTxTs.foreach(lineTxT=>{
        md5list.add(lineTxT.split(this.md5split).head)
      })
    }
    md5list
  }

  private def getbreakTime( splitFile: String): Long = {
    if(this.breakTime0 == Long.MaxValue){
      this.breakTime0=sparksubmit_PropertiesUtil.breakTime(splitFile)
    }
    this.breakTime0
  }

  private val sdf = new SimpleDateFormat("yyyyMMdd")


  private val DateS = sdf.format(new Date)


  private var MD5RecordFile0:String=null
  private def getMD5RecordFile (splitFile:String): String ={

    if(this.MD5RecordFile0 == null){
      val md5Path=this.getbaseMd5Path(splitFile) +"/"+Analysis.getRegion(splitFile)
      if(!HDFSOperation1.exists(md5Path)){
        HDFSOperation1.mkdirs(md5Path)
      }
      this.MD5RecordFile0 =md5Path+ "/"+ DateS + ".dat"
    }
    this.MD5RecordFile0
  }

  def main(args:Array[String]): Unit ={
    sparksubmit_PropertiesUtil.log_monitorHDFS.info("From monitorHDFS.main ...")
    val splitFile=args(0)

    this.HDFSLister(splitFile)
  }


  def HDFSLister(splitFile:String): Unit ={
    val now = System.currentTimeMillis
    //    val day = sdf.format(date)
    val md5list=this.getmd5list(splitFile)
    val md5Path=this.getMD5RecordFile(splitFile)

    var submitOrNot:Boolean=false

    val eles=this.geteles(splitFile)
    val basePath=this.getbasePath(splitFile)
    val breakTime=this.getbreakTime(splitFile)

    val outer :Breaks= new Breaks
    val inner :Breaks= new Breaks

    outer.breakable(
      eles.foreach(ele=>{
        val paths:String = basePath +"/"+ ele + "/" + DateS + "/"
        if(!HDFSOperation1.exists(paths)){
          sparksubmit_PropertiesUtil.log_monitorHDFS.info("mkdir:"+paths)
          HDFSOperation1.mkdirs(paths)
        }else{
          sparksubmit_PropertiesUtil.log_monitorHDFS.info("monitor:"+paths)
          val filenames:ListBuffer[String]=HDFSOperation1.listChildrenAbsoluteFile(paths)
          inner.breakable(
            if(filenames.size>0){
              filenames.foreach(filename=>{
                val md5:String=HDFSFile.fileMD5(filename)
                val mtime =HDFSFile.fileModificationTime(filename)
                if(!md5list.contains(md5) & (now - mtime <= breakTime*1000)){
                  val md5_kv = md5 + this.md5split + filename
                  val flag0=HDFSReadWriteUtil.writeTXT(md5Path,md5_kv,true)
                  if(flag0){
                    sparksubmit_PropertiesUtil.log_monitorHDFS.info("write MD5,md5_kv="+md5_kv+"(md5Path="+md5Path+")")

                    val signalMsgs=HDFSReadWriteUtil.readTXT(filename)
                    if(signalMsgs.length>1){
                      val msg="目前仅支持单文件中存在单信息结构"
                      sparksubmit_PropertiesUtil.log_monitorHDFS.warn(msg)
                    }
                    signalMsgs.foreach(signalMsg=>{
                      //***************************区域特殊性
                      val (dataType: String, applicationTime: Date, generationFileName: String, timeStamp: Date, attribute: String) = JsonStream.analysisJsonStream(signalMsg)
                      val regionKeyContain:Boolean={
                        //                        if(PropertiesUtil.regionKeyOpen){
                        if(dataType.startsWith("reduceLatLon") && !splitFile.contains(platformRole.getregionKey_online)){
                          false
                        }else{
                          true
                        }
                        //                        }else{
                        //                          false
                        //                        }
                      }
                      //***************************区域特殊性

                      val dataTypeOpen=ConfUtil.getdataTypeOpen(signalMsg,splitFile)
                      if(dataTypeOpen){
                        if(regionKeyContain){
                          val msg="任务提交signalMsg="+signalMsg+";splitFile="+splitFile
                          sparksubmit_PropertiesUtil.log_monitorHDFS.info(msg)
                          val msg0="触发信号的文件任务提交signalFile为"+filename
                          sparksubmit_PropertiesUtil.log_monitorHDFS.info(msg0)
                          MDC.put(Constants.dataTypeKey,dataType)
                          submitOrNot=sparksubmit.jobsubmit_byMsg_sparksubmitOpen(signalMsg,splitFile,Array(signalMsg,splitFile))

                        }else{
                          val msg="设置跳过任务提交,regionKey="+platformRole.getregionKey_online+";signalMsg="+signalMsg+";splitFile="+splitFile
                          sparksubmit_PropertiesUtil.log_monitorHDFS.warn(msg)
                        }

                        outer.break()
                      }else{
                        val msg="信号任务关闭,dataTypeOpen="+dataTypeOpen+";不通过监控脚本启动。可手动执行"
                        sparksubmit_PropertiesUtil.log_monitorHDFS.warn(msg)
                      }
                    })
                  }
                }
              })
            }
          )
        }
      })
    )

    val msg:String={
      if(submitOrNot){
        "本次扫描,提交任务"
      }else{
        "本次扫描,未提交任务"
      }
    }
    sparksubmit_PropertiesUtil.log_monitorHDFS.info(msg)

  }
}
