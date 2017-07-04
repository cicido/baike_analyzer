package baike_synonym

import common.{DXPUtils, SparkEnv}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by duanxiping on 2017-06-29
  * 分析从<meta name="keywords">的关键词
  * 1.对词条中不带空格的词进行分析,对于含有空格的情况,暂时先去掉
  * 2.含英文字母不区分大小写,统一转小写
  * 3.词长度大于1
  * to do:
  * 1.带空格词的处理
  * 2.由词条跳转引起的近义词无法去除
  * 如"南头古城	  新安故城 南头古城 深圳新安故城 新安故城历史发展 新安故城气候"
  *  "新安故城	  新安故城 南头古城 深圳新安故城 新安故城历史发展"
  * 导致某些近义词无法去掉.
  * 但目前可以在代码中进行比较分析,找出真正的同义词,只是可能分析的量会比较大一点.
  */
object AnalyzerSynonym {
  val srcTable = "algo.dxp_label_baike_word_input_dir_meta_keywords"
  val dstTable = "algo.dxp_label_baike_word_input_synonym_dir_mata_keywords"
  val sparkEnv = new SparkEnv("AnalyzerSynonym")

  def main(args: Array[String]): Unit = {
    val dt = args(0)
    val outdt = args(1)
    val debug = args(2).toLowerCase == "true"
    val selectSQL:String = s"select bk_word, bk_input,dir_words,meta_keywords from ${srcTable} " +
      s" where stat_date=${dt}"
    val synoRDD: RDD[Row] = sparkEnv.hiveContext.sql(selectSQL).repartition(800).map(r=>{
      (r.getAs[String](0).trim.toLowerCase,r.getAs[String](1).trim.toLowerCase,
      r.getAs[String](2).trim.toLowerCase,r.getAs[String](3).trim.toLowerCase())
    }).filter(r=>{
        r._1.split("[ \t]+").length == 1
      }).map(r=>{
      val word:String = try {
        r._1.split("/")(0)
      }catch {
        case e:Exception => r._1
      }
      val inputWord:String = r._2
      val dirWords = if(r._3.length > 0){
        r._3.split(",")
      }else{
        Array[String]()
      }


      // 构造需要过滤的词表,包括word,inputword及这两个词与目录词的组合
      val filterWords:Array[String] =
        if(dirWords.length > 0) {
          if (word == inputWord) {
            dirWords.map(w => word + w)
          } else {
            dirWords.flatMap(w => {
              Array(word + w, inputWord + w)
            })
          }
        }else{
          Array[String]()
        }

      //考虑空格的影响,先把目录词删除,先替换长词,再替换短词.
      //先把完全匹配的去掉,再去掉匹配目录词的
      var metaKeywords:String = r._4
      if (filterWords.length > 0) {
        (filterWords.sortWith(_.length > _.length) ++ dirWords.sortWith(_.length>_.length)).foreach(w => {
          metaKeywords = metaKeywords.replace(w, " ")
        })
      }
      val syno = metaKeywords.split("[ \t]+").distinct.filter(w=>(w != word && w != inputWord))
      Row(word,inputWord,syno.mkString(","),dirWords.mkString(","),r._4)
    })
    val st: StructType = DXPUtils.createStringSchema(
      Array("bk_word", "bk_input","syno","dir_words","meta_keywords"))
    val df: DataFrame = sparkEnv.hiveContext.createDataFrame(synoRDD, st)
    DXPUtils.saveDataFrame(df, dstTable, outdt, sparkEnv.hiveContext)

  }
}
