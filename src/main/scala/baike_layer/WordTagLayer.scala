package baike_layer

import common.{DXPUtils, SparkEnv}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by duanxiping on 2017/2/6.
  * Updated: 2017-06-21 bug:层次对应的数字有问题,暂未解决
  */
object WordTagLayer {
  val srcTable = "algo.dxp_label_baike_words_filter"
  val dstTable = "algo.dxp_label_baike_word_tag_layer"
  val sparkEnv = new SparkEnv("WordTagLayer")

  def main(args: Array[String]): Unit = {
    val dt = args(0)
    val debug = args(1).toLowerCase == "true"

    val tagLayerArr: Array[(Int, String, Array[String])] = sparkEnv.sc.textFile("/tmp/duanxiping/multilayer.txt").map(r => {
      val arr = r.trim.split(">")
      val arrlen = arr.length
      try {
        (arrlen, arr(arrlen - 1), arr)
      } catch {
        case e: Exception => (0, "", Array(""))
      }
    }).collect().filter(_._1 != 0)

    //val tag2LayIdx: Map[String, Int] = tagLayerArr.map(r => (r._2, r._1)).toMap
    val tag2AllTag: Map[String, Array[String]] = tagLayerArr.map(r => (r._2, r._3)).toMap

    val selectSQL = s"select bk_word,bk_tags from ${srcTable}"

    val wordRDD: RDD[Row] = sparkEnv.hiveContext.sql(selectSQL).
      repartition(80).map(r => {
      val word: String = r.getAs[String](0)
      val tags: Array[String] = r.getAs[String](1).split(",")
      //找出各个tag对应到的层级
      val tagsArr = tags.map(w => {
        try {
          (w, tag2AllTag(w))
        } catch {
          case e: Exception => (w, Array(""))
        }
      }).filter(_._2.length != 0).map(t => {
        val matchArr = tags.filter(w => t._2.contains(w))
        (t._1, t._2, matchArr)
      }).sortWith(_._3.length > _._3.length)

      // word, tags, 最长链条的末位tag,最长tag链条,未包含在层次的标签,tags所处层级
      Row(word, tags.mkString(","), tagsArr(0)._1, tagsArr(0)._2.mkString("-"),
        tags.filterNot(tagsArr(0)._3.contains(_)).mkString(","),
        tagsArr.map(w => (w._1 + "/" + w._2.length)).mkString(",")
      )
    })

    val st: StructType = DXPUtils.createStringSchema(Array("word", "tags", "endtag",
      "taglayer", "unmatchtag", "tagidx"))
    val df: DataFrame = sparkEnv.hiveContext.createDataFrame(wordRDD, st)
    DXPUtils.saveDataFrame(df, dstTable, dt, sparkEnv.hiveContext)
  }
}
