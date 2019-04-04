package com.weather.bigdata.it.spark

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.CountDownLatch

import com.weather.bigdata.it.spark.platform.signal.JsonStream
import com.weather.bigdata.it.utils.hdfsUtil.{HDFSOperation1, HDFSReadWriteUtil, MyPathFilterInterface}
import org.apache.hadoop.fs.Path
import org.apache.spark.launcher.{SparkAppHandle, SparkLauncher}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}


object sparksubmit {
  private val sdf = new SimpleDateFormat("yyyyMMdd")
  private val DateS = sdf.format(new Date)
  private val sparksubmitOpen: Boolean = PropertiesUtil.sparksubmitOpen

  //  def getRegion(splitFile:String): String ={
  //    val uri = URI.create(splitFile)
  //    val pathName=new Path(uri)
  //    val kkk=pathName.getParent.getName
  //    val region=kkk.split("\\.").head
  //    region
  //  }
  def getlogPath(splitFile:String): String ={
    val logPath0 = PropertiesUtil.logPath + "/" + PropertiesUtil.getRegion(splitFile) + "/" + DateS
    if(!HDFSOperation1.exists(logPath0)){
      HDFSOperation1.mkdirs(logPath0)
    }
    logPath0
  }

  def cuthead(arr:Array[String]):Array[String] ={
    val newarr:ArrayBuffer[String]=new ArrayBuffer[String]()
    for(i<- 1 until arr.length){
      newarr += arr(i)
    }
    newarr.toArray
  }
  class jarfilter extends MyPathFilterInterface{
    override def accept (path: Path): Boolean = {
      path.toString.endsWith(".jar")
    }
  }

  //  def jobsubmit_byFile(signalFile: String, splitFile: String, args: Array[String]):Unit={
  //    val signalMsgs=HDFSReadWriteUtil.readTXT(signalFile)
  //    signalMsgs.foreach(signalMsg=>{
  //      this.jobsubmit_byMsg(signalMsg,splitFile,args)
  //    })
  //  }
  def jobsubmit_byMsg(signalMsg: String,splitFile: String,args:Array[String]): Unit ={
    if(sparksubmitOpen){
      val timeStamp=System.currentTimeMillis()
      //添加心跳日志输出
      val logpath = PropertiesUtil.logPath + "/" + PropertiesUtil.getRegion(splitFile) + "/" + DateS + "/"
      HDFSOperation1.mkdirs(logpath)
      val logfile=new File(logpath+"/"+timeStamp+".log")

      val par="(logfile="+logfile.toString+";signalMsg="+signalMsg+",splitFile="+splitFile+")"

      val mainJar: String = PropertiesUtil.getmainJar(signalMsg, splitFile)
      val mainClass1: String = PropertiesUtil.getmainClass1(signalMsg)
      val mainClass0: String = PropertiesUtil.mainClass0
      val libsPath: String = PropertiesUtil.getlibPath(signalMsg, splitFile)

      val SPARK_HOME=PropertiesUtil.SPARK_HOME
      val JAVA_HOME=PropertiesUtil.JAVA_HOME
      val Master=PropertiesUtil.Master
      val DeployMode=PropertiesUtil.DeployMode
      val resources: String = PropertiesUtil.sparksubmit_config_out

      val libs:ListBuffer[String]=HDFSOperation1.listChildrenAbsoluteFile(libsPath,new ListBuffer[String],new jarfilter)

      val dataType: String = JsonStream.analysisDataType(signalMsg)
      val APPNAME=dataType
      val (driverMemorry:String,executorMemorry:String,executorNum:String)=ResourceConf.getResource(signalMsg,splitFile)

      val msg=dataType + "的资源类型,使用配置("+driverMemorry+","+executorMemorry+","+executorNum+"),"+par
      PropertiesUtil.log.info(msg)

      val lunch:SparkLauncher=new SparkLauncher()
        .setAppName(APPNAME)
        .setSparkHome(SPARK_HOME)
        .setJavaHome(JAVA_HOME)
        .setMaster(Master)
        .setDeployMode(DeployMode)
        .setAppResource(mainJar)

        //        .setMainClass(mainClass1)
        .setMainClass(mainClass0)
        .setConf(SparkLauncher.DRIVER_MEMORY,driverMemorry)
        .setConf(SparkLauncher.EXECUTOR_MEMORY,executorMemorry)
        //.setConf("spark.executor.cores",executorNum)
        .setConf("spark.executor.instances",executorNum)
        .setPropertiesFile(resources)
        .setVerbose(true)
      libs.foreach(lib=>{
        lunch.addJar(lib)
      })

      lunch.addAppArgs(mainClass1)
      args.foreach(arg=>{
        lunch.addAppArgs(arg)
      })


      lunch.redirectOutput(logfile)

      val countDownLatch : CountDownLatch= new CountDownLatch(1)
      val handle:SparkAppHandle=lunch.startApplication(new SparkAppHandle.Listener {
        override def infoChanged (handle: SparkAppHandle): Unit = {
          //        val appId=handle.getAppId
          //        val msg="appId="+ appId + handle.getState().toString()+"("+signalMsg+","+splitFile+")"
          //        PropertiesUtil.log.info(msg)
        }
        override def stateChanged (handle: SparkAppHandle): Unit = {
          val appId=handle.getAppId
          val msg= appId + ";state="+handle.getState().toString()+par
          PropertiesUtil.log.info(msg)
          if (handle.getState().isFinal()) {
            countDownLatch.countDown()
          }
        }
      })

      val msg0="The task is executing, please wait ...."
      PropertiesUtil.log.info(msg0)
      countDownLatch.await()
      val appId=handle.getAppId
      val state=handle.getState
      val msg1=appId+" is finished!finalState="+state
      PropertiesUtil.log.info(msg1)
    }else{
      val msg="sparksubmitOpen="+sparksubmitOpen
      PropertiesUtil.log.info(msg)
    }
  }

  //  def jobsubmit_byMsgJson(signalMsgJSON: JSONObject, splitFile: String, args:Array[String]): Unit ={
  //    if(sparksubmitOpen){
  //      val timeStamp=System.currentTimeMillis()
  //      //添加心跳日志输出
  //      val logpath=PropertiesUtil.prop.getProperty("logPath")+"/"+PropertiesUtil.getRegion(splitFile)+"/"+DateS+"/"
  //      HDFSOperation1.mkdirs(logpath)
  //      val logfile=new File(logpath+"/"+timeStamp+".log")
  //
  //      val par="(logfile="+logfile.toString+";signalMsg="+signalMsgJSON+",splitFile="+splitFile+")"
  //
  //      val mainJar:String=PropertiesUtil.getmainJar(splitFile)
  //      val mainClass:String=PropertiesUtil.mainClass
  //      val libsPath:String=PropertiesUtil.getlibPath(splitFile)
  //
  //      val SPARK_HOME=PropertiesUtil.SPARK_HOME
  //      val JAVA_HOME=PropertiesUtil.JAVA_HOME
  //      val Master=PropertiesUtil.Master
  //      val DeployMode=PropertiesUtil.DeployMode
  //
  //      val libs:ListBuffer[String]=HDFSOperation1.listChildrenAbsoluteFile(libsPath,new ListBuffer[String],new jarfilter)
  //
  //      val dataType: String = JsonStream.analysisDataType(signalMsgJSON)
  //      val APPNAME=dataType
  //      val (driverMemorry:String,executorMemorry:String,executorNum:String)=ResourceConf.getResource(signalMsgJSON,splitFile)
  //
  //      val msg=dataType + "的资源类型,使用配置("+driverMemorry+","+executorMemorry+","+executorNum+"),"+par
  //      PropertiesUtil.log.info(msg)
  //
  //      val lunch:SparkLauncher=new SparkLauncher()
  //        .setAppName(APPNAME)
  //        .setSparkHome(SPARK_HOME)
  //        .setJavaHome(JAVA_HOME)
  //        .setMaster(Master)
  //        .setDeployMode(DeployMode)
  //        .setAppResource(mainJar)
  //        .setMainClass(mainClass)
  //        .setConf(SparkLauncher.DRIVER_MEMORY,driverMemorry)
  //        .setConf(SparkLauncher.EXECUTOR_MEMORY,executorMemorry)
  //        //.setConf("spark.executor.cores",executorNum)
  //        .setConf("spark.executor.instances",executorNum)
  //        .setVerbose(true)
  //      libs.foreach(lib=>{
  //        lunch.addJar(lib)
  //      })
  //
  //      args.foreach(arg=>{
  //        lunch.addAppArgs(arg)
  //      })
  //
  //
  //      lunch.redirectOutput(logfile)
  //
  //      val countDownLatch : CountDownLatch= new CountDownLatch(1)
  //      val handle:SparkAppHandle=lunch.startApplication(new SparkAppHandle.Listener {
  //        override def infoChanged (handle: SparkAppHandle): Unit = {
  //          //        val appId=handle.getAppId
  //          //        val msg="appId="+ appId + handle.getState().toString()+"("+signalMsg+","+splitFile+")"
  //          //        PropertiesUtil.log.info(msg)
  //        }
  //        override def stateChanged (handle: SparkAppHandle): Unit = {
  //          val appId=handle.getAppId
  //          val msg= appId + ";state="+handle.getState().toString()+par
  //          PropertiesUtil.log.info(msg)
  //          if (handle.getState().isFinal()) {
  //            countDownLatch.countDown()
  //          }
  //        }
  //      })
  //
  //      val msg0="The task is executing, please wait ...."
  //      PropertiesUtil.log.info(msg0)
  //      countDownLatch.await()
  //      val appId=handle.getAppId
  //      val state=handle.getState
  //      val msg1=appId+" is finished!finalState="+state
  //      PropertiesUtil.log.info(msg1)
  //    }else{
  //      val msg="sparksubmitOpen="+sparksubmitOpen
  //      PropertiesUtil.log.info(msg)
  //    }
  //  }
  //  def main(args:Array[String]): Unit ={
  //    if(args.length<2){
  //      val msg="sparksubmit提交初始参数有误args(0)[signalFile],args(1)[splitFile]...."
  //      PropertiesUtil.log.error(msg)
  //    }else{
  //      val signalFile=args(0)
  //      val splitFile=args(1)
  //      val args0:Array[String]={
  //        if(args.length>2){
  //          this.cuthead(this.cuthead(args))
  //        }else{
  //          Array.empty[String]
  //        }
  //      }
  //      val signalMsgs=HDFSReadWriteUtil.readTXT(signalFile)
  //      signalMsgs.foreach(signalMsg=>{
  //        this.jobsubmit_byMsg(signalMsg,splitFile,args0)
  //      })
  //    }
  //  }
  def main (args: Array[String]): Unit = {
    println("from main")
    val signalFile = args(0)
    val signalMsgs: Array[String] = HDFSReadWriteUtil.readTXT(signalFile)
    val splitFile = args(1)
    signalMsgs.foreach(signalMsg => {
      this.jobsubmit_byMsg(signalMsg, splitFile, Array(signalMsg, splitFile))
    })

  }
}
