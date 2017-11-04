package com.homework.arragehdfsdir

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}



object HDFSUtil {
  def arrangeDir(): Unit = {
    val targetPath = "hdfs://192.168.128.91:9000/spark/emp/temp/201711112025/d=20171111/h=20/"
    val hdfsPath = "hdfs://192.168.128.91:9000/"
    val conf = new Configuration();
    val fs = FileSystem.get(new URI(hdfsPath), conf);
    try {
      val listFiles = fs.listFiles(new Path(targetPath), false)
      var n = 1

      while(listFiles.hasNext) {
        val x = listFiles.next()
        if (!x.getPath.getName.contains("_SUCCESS" )) {
          println("origin path: " + x.getPath)

          val length = "hdfs://192.168.128.91:9000/spark/emp/temp/".length
          val dirs: Array[String] = x.getPath.toString.substring(length).split("/")

          var filename = dirs(0).substring(2).replace(dirs(2).substring(2), "")
          var newPath = "hdfs://192.168.128.91:9000/spark/emp/data/d=" + dirs(1).substring(4) + "/" + dirs(2)


          if (n < 10){
            filename = filename + "-" + "0" + n
          }else{
            filename = filename + "-" + n
          }
          val finalpath = newPath+ "/" + filename + ".txt"
          println("new Path: " + finalpath)
          fs.create(new Path(finalpath))

          n+=1
        }
      }
    } finally {
      fs.close()
    }
  }
}
