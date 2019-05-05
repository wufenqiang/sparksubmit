package com.weather.bigdata.it.spark.sparksubmit.fileFilter

import com.weather.bigdata.it.utils.hdfsUtil.MyPathFilterInterface
import org.apache.hadoop.fs.Path

class jarfilter extends MyPathFilterInterface{
  override def accept (path: Path): Boolean = {
    path.toString.endsWith(".jar")
  }
}
