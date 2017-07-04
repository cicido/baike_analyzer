package baike_tag

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
object ParseFromHtml {
  val dstTable = "algo.dxp_label_baike_word_relwords_tags"
  val sparkEnv = new SparkEnv("BaikeTag")

  def main(args: Array[String]): Unit = {
    val dt = args(0)
    val srcPath = args(1)
    val debug = args(2).toLowerCase == "true"
    val tagRDD: RDD[Row] = sparkEnv.sc.wholeTextFiles(srcPath, 2000).
      repartition(800).flatMap(r => {
      r._2.split("\n")
    }).map(r => {
      val arr: Array[String] = r.split("\001")
      if (arr.length == 1) {
        Row(arr(0), "none", "none")
      }else {
        val doc: Element = Jsoup.parse(arr(1)).body()
        val links: Array[String] =
          try {
            doc.select("a[href]").filter(l => {
              val href: String = l.attr("href")
              href.startsWith("/item") &&
                !href.endsWith("fr=navbar") &&
                !href.endsWith("force=1")
            }).map(l => {
              l.text().replace("#viewPageContent", "")
            }).toArray
          } catch {
            case e: Exception => Array("")
          }
        val tags: Array[String] =
          try {
            doc.select("span.taglist").map(t => {
              t.text()
            }).toArray
          } catch {
            case e: Exception => Array("")
          }
        Row(arr(0), links.mkString(","), tags.mkString(","))
      }
    })

    val st: StructType = DXPUtils.createStringSchema(Array("bk_word", "bk_relwords", "bk_tags"))
    val df: DataFrame = sparkEnv.hiveContext.createDataFrame(tagRDD, st)
    DXPUtils.saveDataFrame(df, dstTable, dt, sparkEnv.hiveContext)

  }
}
