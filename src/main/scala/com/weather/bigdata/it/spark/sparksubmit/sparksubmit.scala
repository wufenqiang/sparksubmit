package com.weather.bigdata.it.spark.sparksubmit

import java.util.concurrent.CountDownLatch

import com.weather.bigdata.it.spark.platform.signal.JsonStream
import com.weather.bigdata.it.spark.sparksubmit.fileFilter.{jarfilter, propertiesfilter}
import com.weather.bigdata.it.utils.hdfsUtil.{HDFSFile, HDFSOperation1, HDFSReadWriteUtil}
import org.apache.log4j.MDC
import org.apache.spark.launcher.{SparkAppHandle, SparkLauncher}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}


object sparksubmit {
  private def getsparksubmitOpen(splitFile: String):Boolean=sparksubmit_PropertiesUtil.sparksubmitOpen(splitFile)


  //  private var applicationId:String=""
  //  def getlogPath(splitFile:String): String ={
  //    val logPath0 = PropertiesUtil.log_detail_PathRoot + "/" + PropertiesUtil.getRegion(splitFile) + "/" + DateS
  //    if(!HDFSOperation1.exists(logPath0)){
  //      HDFSOperation1.mkdirs(logPath0)
  //    }
  //    logPath0
  //  }

  private def cuthead(arr:Array[String]):Array[String] ={
    val newarr:ArrayBuffer[String]=new ArrayBuffer[String]()
    for(i<- 1 until arr.length){
      newarr += arr(i)
    }
    newarr.toArray
  }

  //  def jobsubmit_byFile(signalFile: String, splitFile: String, args: Array[String]):Unit={
  //    val signalMsgs=HDFSReadWriteUtil.readTXT(signalFile)
  //    signalMsgs.foreach(signalMsg=>{
  //      this.jobsubmit_byMsg(signalMsg,splitFile,args)
  //    })
  //  }

  private def jobsubmit_byMsg(signalMsg: String,splitFile: String,args:Array[String]): Unit ={

    sparksubmit_PropertiesUtil.log_sparksubmit.warn(System.getenv)
    sparksubmit_PropertiesUtil.log_sparksubmit.warn(System.getProperties)

    val par="signalMsg="+signalMsg+",splitFile="+splitFile+")"

    val mainJar: String = sparksubmit_PropertiesUtil.getmainJar(signalMsg, splitFile)
    //      val mainClass1: String = PropertiesUtil.getmainClass1(signalMsg)
    val mainClass0: String = sparksubmit_PropertiesUtil.mainClass0

    val platformlibPath: String = sparksubmit_PropertiesUtil.getplatformlibPath(splitFile)
    val signallibPath:String=sparksubmit_PropertiesUtil.getsignallibPath(signalMsg,splitFile)
    val signallibPath_common:String=sparksubmit_PropertiesUtil.getmt_commonsLibPath(splitFile)

    val SPARK_HOME=sparksubmit_PropertiesUtil.SPARK_HOME
    val JAVA_HOME=sparksubmit_PropertiesUtil.JAVA_HOME
    val Master=sparksubmit_PropertiesUtil.Master
    val DeployMode=sparksubmit_PropertiesUtil.DeployMode
    //    val resources_out_it: String = PropertiesUtil.getSparkSubmit_config_out_it(splitFile)
    //    val resources_out_mt: String = PropertiesUtil.getSparkSubmit_config_out_mt(splitFile)

    val platformlibs:ListBuffer[String]=HDFSOperation1.listChildrenAbsoluteFile(platformlibPath,new ListBuffer[String],new jarfilter)
    val signallibs:ListBuffer[String]=HDFSOperation1.listChildrenAbsoluteFile(signallibPath,new ListBuffer[String],new jarfilter)
    val signallibs_commons:ListBuffer[String]=HDFSOperation1.listChildrenAbsoluteFile(signallibPath_common,new ListBuffer[String],new jarfilter)

    val platformproperties:ListBuffer[String]=HDFSOperation1.listChildrenAbsoluteFile(platformlibPath,new ListBuffer[String],new propertiesfilter)
    val signalproperties:ListBuffer[String]=HDFSOperation1.listChildrenAbsoluteFile(signallibPath,new ListBuffer[String],new propertiesfilter)
    val signalproperties_commons:ListBuffer[String]=HDFSOperation1.listChildrenAbsoluteFile(signallibPath_common,new ListBuffer[String],new propertiesfilter)

    val dataType: String = JsonStream.analysisDataType(signalMsg)
    MDC.put(Constants.dataTypeKey,dataType)
    val APPNAME=dataType

    val (driverMemorry:String,executorMemorry:String,executorNum:Int)=ConfUtil.getdataTypeResource(signalMsg,splitFile)
    val mainClass1=ConfUtil.getdataTypeMainClass(signalMsg,splitFile)


    val msg=dataType + "的资源类型,使用配置("+driverMemorry+","+executorMemorry+","+executorNum+"),"+par
    sparksubmit_PropertiesUtil.log_sparksubmit.info(msg)

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
      .setConf("spark.executor.instances",executorNum.toString)
      //
      //.setPropertiesFile(resources_out)//hdfs上的文件会报错
      //上传resources_out_mt,入库读取eachpartition=5时用到
      //      .addFile(resources_out_mt)
      .setVerbose(true)

    //添加平台内置第三方jar包
    //添加基于信号的commons包
    //添加基于信号的计算方法实现类
    val libs=platformlibs.union(signallibs_commons).union(signallibs)
    libs.foreach(lib=>{
      val libMD5=HDFSFile.fileMD5(lib)
      sparksubmit_PropertiesUtil.log_sparksubmit.info("spark任务上传依赖="+lib+";md5="+libMD5)
      lunch.addJar(lib)
    })

    //添加平台内置依赖配置
    //添加基于信号的commons 依赖配置
    //添加基于信号的计算方法实现类 依赖配置
    val properties=platformproperties.union(signalproperties_commons).union(signalproperties)
    properties.foreach(property=>{
      val propertyMD5=HDFSFile.fileMD5(property)
      sparksubmit_PropertiesUtil.log_sparksubmit.info("spark任务上传配置="+property+";md5="+propertyMD5)
      lunch.addFile(property)
    })

    //方法接入入口当参数传入
    lunch.addAppArgs(mainClass1)
    //添加参数
    args.foreach(arg=>{
      lunch.addAppArgs(arg)
    })


    if(sparksubmit_PropertiesUtil.sparksubmit_Detail_Open(splitFile)){
      //链接心跳输出日志模式
      //    val logfile=PropertiesUtil.sparksubmit_Detail_File
      val loggername=sparksubmit_PropertiesUtil.sparksubmit_logName
      //重定向心跳日志输出与时间戳有关
      sparksubmit_PropertiesUtil.set_sparksubmit_Detail(splitFile)
      //日志模式与提交后任务详情链接
      lunch.redirectToLog(loggername)

      //    PropertiesUtil.log_sparksubmit.info("logfile.getAbsolutePath="+logfile.getAbsolutePath)


      //    lunch.setConf(SparkLauncher.CHILD_PROCESS_LOGGER_NAME,logfilename)

      //    lunch.redirectOutput(logfile:File)
      //    lunch.redirectOutput({
      //      Redirect.to(logfile)
      //    })
      //    lunch.redirectOutput({
      //      Redirect.from(logfile)
      //    })
      //    lunch.redirectOutput({
      //      Redirect.appendTo(logfile)
      //    })
    }




    val countDownLatch : CountDownLatch= new CountDownLatch(1)
    val handle:SparkAppHandle=lunch.startApplication(new SparkAppHandle.Listener {
      override def infoChanged (handle: SparkAppHandle): Unit = {
        val appId=handle.getAppId
        if(appId!="" && appId!=null && !MDC.getContext.containsKey(Constants.applicationIdKey)){
          MDC.put(Constants.applicationIdKey,appId)
        }
      }
      override def stateChanged (handle: SparkAppHandle): Unit = {

        val appId=handle.getAppId
        val handlestate=handle.getState()
        val stateStr=handlestate.toString()
        if(stateStr!="" && stateStr!=null){
          MDC.put(Constants.stateKey,stateStr)
        }

        //        MDC.put(applicationIdKey,appId)
        //        MDC.put(stateKey,stateStr)

        val msg= appId+";state="+stateStr+";"+par
        sparksubmit_PropertiesUtil.log_sparksubmit.info(msg)
        if (handlestate.isFinal()) {
          countDownLatch.countDown()
        }
      }
    })

    val msg0="The task is executing, please wait ...."
    sparksubmit_PropertiesUtil.log_sparksubmit.info(msg0)
    countDownLatch.await()
    val appId=handle.getAppId
    val state=handle.getState
    val msg1=appId+".finalState="+state
    sparksubmit_PropertiesUtil.log_sparksubmit.info(msg1)
    if(state.isFinal){
      //      HDFSOperation1.copyfile(logfile,hdfslo)
    }
  }

  def jobsubmit_byMsg_sparksubmitOpen(signalMsg: String,splitFile: String,args:Array[String]): Boolean ={
    if(this.getsparksubmitOpen(splitFile)){
      val dataType: String = JsonStream.analysisDataType(signalMsg)

      this.jobsubmit_byMsg(signalMsg,splitFile,args)
      true
    }else{
      val msg="sparksubmitOpen="+this.getsparksubmitOpen(splitFile)
      sparksubmit_PropertiesUtil.log_sparksubmit.info(msg)
      false
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
    sparksubmit_PropertiesUtil.log_sparksubmit.info("From sparksubmit.main ...")
    val signalFile = args(0)
    val signalMsgs: Array[String] = HDFSReadWriteUtil.readTXT(signalFile)
    val splitFile = args(1)
    signalMsgs.foreach(signalMsg => {
      this.jobsubmit_byMsg(signalMsg, splitFile, Array(signalMsg, splitFile))
    })

  }
}
