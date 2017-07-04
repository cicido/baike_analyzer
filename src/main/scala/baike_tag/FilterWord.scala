package baike_tag

import common.{DXPUtils, SparkEnv}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}


/**
  * Created by duanxiping on 2017/2/6.
  */
object FilterWord {
  val srcTable = "algo.dxp_label_baike_word_relwords_tags"
  val dstTable = "algo.dxp_label_baike_words_filter"
  val sparkEnv = new SparkEnv("WordFilter")

  /*
    to avoid the following code:
    scala> "/".split("/")(0)
    scala> "//".split("/")(0)
    will cause: java.lang.ArrayIndexOutOfBoundsException: 0
   */

  def main(args: Array[String]): Unit = {
    val dt = args(0)
    val debug = args(1).toLowerCase == "true"

    val selectSQL = s"select bk_word,bk_relwords,bk_tags from ${srcTable}"
    //设置过滤条件
    val wordRDD: RDD[Row] = sparkEnv.hiveContext.sql(selectSQL).
      repartition(800).map(r => {
      val word: String = r.getAs[String](0).trim
      val relwordsArr: Array[String] = r.getAs[String](1).trim.split(",")
      val tags: String = r.getAs[String](2).trim
      (word, relwordsArr,tags)
    }).filter(r => {
      r._1.length < 24 && r._1.length > 1 &&
        !r._1.startsWith("/") &&
        r._2.filter(w => w.length > 0).length > 2 &&
        r._3.length > 0 && r._3.split(",").length > 0
    }).map(r => {
      val word: String = try {
        r._1.split("/")(0)
      } catch {
        case e: Exception => {
          e.printStackTrace()
          println("*\n" * 10 + r._1)
          r._1
        }
      }
      val relwords: Array[String] = r._2.filter(w => {
        !w.startsWith("/") && w.length > 0
      }).map(w => {
        try {
          w.split("/")(0)
        } catch {
          case e: Exception => w
        }
      })
      (word, (relwords, r._3))
    }).reduceByKey((a, b) => {
      //这里加深了对reduceByKey的认识,去掉一词多义,这里可以再优化
      if (a._1.length > b._1.length)
        a
      else
        b
    }).map(r=>{
      Row(r._1,r._2._1.mkString(","),r._2._2)
    })

    val st: StructType = DXPUtils.createStringSchema(Array("bk_word","bk_relwords","bk_tags"))
    val df: DataFrame = sparkEnv.hiveContext.createDataFrame(wordRDD,st)
    DXPUtils.saveDataFrameWithOutPartition(df, dstTable, sparkEnv.hiveContext)

  }

}
