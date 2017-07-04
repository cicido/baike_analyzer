package baike_synonym

import common.{DXPUtils, SparkEnv}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}
import org.jsoup.Jsoup
import org.jsoup.nodes.Document

import scala.collection.JavaConversions._

/**
  * Created by duanxiping on 2017-06-29
  * 从爬取的数据中提取同义词.解析<meta keywords>
  * to do:
  * 为方便去掉非同义词,应该把页面的目录部分进行解析,用于页面去掉
  */
object ParseSynonymFromHtml {
  val srcTable = "algo.dxp_label_baike_word_html"
  val dstTable = "algo.dxp_label_baike_word_input_dir_meta_keywords"
  val sparkEnv = new SparkEnv("BaikeTag")

  def main(args: Array[String]): Unit = {
    val dt = args(0)
    val outdt = args(1)
    val debug = args(2).toLowerCase == "true"
    val selectSQL = s"select bk_word,bk_html from ${srcTable}"
    val tagRDD: RDD[Row] = sparkEnv.hiveContext.sql(selectSQL).map(r => {
      val bk_word: String = r.getAs[String](0)
      val bk_html: String = r.getAs[String](1)

      val htmlParser: Document = Jsoup.parse(bk_html)
      val doc = htmlParser.body()

      val inputWord: String =
        try {
          doc.select("input#query")(0).attr("value")
        } catch {
          case e: Exception => {
            e.printStackTrace()
            ""
          }
        }

      val dirWords: Array[String] =
        try {
          val dirEle = doc.select("div.lemmaWgt-lemmaCatalog")
          dirEle.select("a").filter(l => {
            val href = l.attr("href")
            href.startsWith("#") && !href.contains("_")
          }).map(l => l.text()).toArray
        } catch {
          case e: Exception => {
            e.printStackTrace()
            Array("")
          }
        }

      val metaKw: Array[String] =
        try {
          htmlParser.select("meta").filter(l => {
            l.attr("name").trim.toLowerCase() == "keywords"
          }).map(l => l.attr("content").trim).toArray
        } catch {
          case e: Exception => {
            e.printStackTrace()
            Array("")
          }
        }
      Row(bk_word, inputWord, metaKw.mkString(","), dirWords.mkString(","))
    })
    val st: StructType = DXPUtils.createStringSchema(Array("bk_word", "bk_input","meta_keywords","dir_words"))
    val df: DataFrame = sparkEnv.hiveContext.createDataFrame(tagRDD, st)
    DXPUtils.saveDataFrame(df, dstTable, outdt, sparkEnv.hiveContext)
  }

}
