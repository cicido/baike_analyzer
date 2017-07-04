package baike_srcdata

import common.{DXPUtils, SparkEnv}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}
import org.jsoup.Jsoup
import org.jsoup.nodes.Element

import scala.collection.JavaConversions._

/**
  * Created by duanxiping on 2017/2/6.
  */
object HdfsToHive {
  val dstTable = "algo.dxp_label_baike_word_html"
  val sparkEnv = new SparkEnv("HdfsToHive")

  def main(args: Array[String]): Unit = {
    val dt = args(0)
    val srcPath = args(1)
    val debug = args(2).toLowerCase == "true"
    val tagRDD: RDD[Row] = sparkEnv.sc.wholeTextFiles(srcPath, 2000).
      repartition(1000).flatMap(r => {
      r._2.split("\n")
    }).map(r => {
      val arr: Array[String] = r.split("\001")
      if (arr.length == 1) {
        Row(arr(0), "none")
      }else {
        Row(arr(0),arr(1))
      }
    })
    val st: StructType = DXPUtils.createStringSchema(Array("bk_word","bk_html"))
    val df: DataFrame = sparkEnv.hiveContext.createDataFrame(tagRDD, st)
    DXPUtils.saveDataFrame(df, dstTable, dt, sparkEnv.hiveContext)

  }
}
