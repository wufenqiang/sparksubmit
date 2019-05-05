package com.weather.bigdata.it.spark.sparksubmit.platformOperation

import com.weather.bigdata.it.spark.sparksubmit.sparksubmit_PropertiesUtil
import com.weather.bigdata.it.utils.hdfsUtil.HDFSOperation1

private object mvjar_core {
  def cpJars(libpath_source: String,libpath_target: String): Boolean ={
    val flag=HDFSOperation1.copyfile(libpath_source,libpath_target,true,false)
    if(flag){
      val msg="libpath_source("+libpath_source+")->libpath_target("+libpath_target+")"
      sparksubmit_PropertiesUtil.log_monitorHDFS.info(msg)
    }else{
      val msg="libpath_source("+libpath_source+")->libpath_target("+libpath_target+")"
      sparksubmit_PropertiesUtil.log_monitorHDFS.error(msg)
    }
    flag
  }
}
