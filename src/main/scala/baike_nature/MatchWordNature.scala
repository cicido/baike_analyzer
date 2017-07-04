package baike_nature

import common.{DXPUtils, SparkEnv}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}


/**
  * Created by duanxiping on 2017/6/27
  * 先给定大类tag 相应的词性，然后对拥有该tag的词条给定相应的词性
  */
object MatchWordNature {
  val srcTable = "algo.dxp_label_baike_word_relwords_tags"
  val dstTable = "algo.dxp_label_baike_words_nature"
  val sparkEnv = new SparkEnv("WordNature")

  /*
    to avoid the following code:
    scala> "/".split("/")(0)
    scala> "//".split("/")(0)
    will cause: java.lang.ArrayIndexOutOfBoundsException: 0
   */

  def main(args: Array[String]): Unit = {
    val dt = args(0)
    val debug = args(1).toLowerCase == "true"

    val tagNatureMap = Map(
      "人物" -> "mznr",
      "语言" -> "mzlang",
      "电影" -> "mzvedio",
      "科学" -> "mzsci",
      "书籍" -> "mzbook",
      "音乐作品" -> "mzmusic",
      "游戏" -> "mzgame",
      "电视剧作品" -> "mzvedio",
      "公司" -> "mzntc",
      "字词" -> "mzchar",
      "科技产品" -> "mzsci",
      "组织机构" -> "mzni",
      "学科" -> "mzsub",
      "生物物种" -> "mzspec",
      "大牌" -> "mzbrand",
      "历史" -> "mzhis",
      "生活" -> "mzlife",
      "行政区划" -> "mzns",
      "品牌" -> "mzbrand",
      "地点" -> "mzns",
      "娱乐作品" -> "mzfun",
      "医学" -> "mzmedi",
      "美术" -> "mzart",
      "文学作品" -> "mzarticle",
      "体育" -> "mzsport",
      "小说" -> "mznovel",
      "娱乐术语" -> "mzfun",
      "食品" -> "mzfood",
      "股票类术语" -> "mzstock",
      "综艺节目" -> "mzshow",
      "二战" -> "mzwar",
      "动画" -> "mzanima",
      "非遗" -> "mzheri",
      "经济术语" -> "mzeco",
      "大学" -> "mzcollege",
      "舞蹈" -> "mzdance",
      "车站" -> "mzstation",
      "奖项" -> "mzmedal",
      "自然资源" -> "mzresource",
      "历史事件" -> "mzhis",
      "本科专业" -> "mzmajor",
      "文化设施" -> "mzculture",
      "专科专业" -> "mzmajor")

    val selectSQL = s"select bk_word,bk_tags from ${srcTable} where bk_tags is not null " +
      s" and bk_tags !='' and length(bk_word)>1 and length(bk_word) < 24"
    //设置过滤条件
    val wordRDD: RDD[Row] = sparkEnv.hiveContext.sql(selectSQL).
      repartition(200).map(r => {
      val word: String =
        try {
          r.getAs[String](0).trim.split("/")(0)
        } catch {
          case e: Exception => {
            e.printStackTrace()
            println("*\n" * 10 + r.getAs[String](0).trim)
            r.getAs[String](0).trim
          }
        }
      val tags: Array[String] = r.getAs[String](1).trim.split(",")
      val nat: Array[String] = tags.map(w => {
        try {
          tagNatureMap(w)
        } catch {
          case e: Exception => "mbk"
        }
      }).filter(_ != "mbk")
      (word, (tags, nat))
    }).filter(r=>r._2._2.length > 0).reduceByKey((a, b) => {
      //这里加深了对reduceByKey的认识,去掉一词多义,这里可以再优化
      if (a._1.length > b._1.length)
        a
      else
        b
    }).map(r => {
      Row(r._1, r._2._1.mkString(","), if(r._2._2.length > 0) r._2._2(0) else "mbk",r._1.length.toString)
    })

    val st: StructType = DXPUtils.createStringSchema(Array("bk_word", "bk_tags", "bk_nature","bk_word_len"))
    val df: DataFrame = sparkEnv.hiveContext.createDataFrame(wordRDD, st)
    DXPUtils.saveDataFrameWithOutPartition(df, dstTable, sparkEnv.hiveContext)
  }

}
