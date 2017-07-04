package baike_layer

import common.{DXPUtils, SparkEnv}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}


/**
  * Created by duanxiping on 2017/2/6.
  */
object TwoTagNumStat {
  val srcTable = "algo.dxp_label_baike_word_relwords_tags"
  val dstTable = "algo.dxp_label_baike_words_two_tag_co_occur"
  val sparkEnv = new SparkEnv("WordRelation")

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
    val wordRDD: RDD[Array[String]] = sparkEnv.hiveContext.sql(selectSQL).
      repartition(800).map(r => {
      val word: String = r.getAs[String](0).trim
      val relwordsArr: Array[String] = r.getAs[String](1).trim.split(",")
      val tagsArr: Array[String] = r.getAs[String](2).trim.split(",")
      (word, relwordsArr, tagsArr)
    }).filter(r => {
      r._1.length < 24 && r._1.length > 0 &&
        !r._1.startsWith("/") &&
        r._2.filter(w => w.length > 0).length > 2 &&
        r._3.length > 0
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
    }).map(r=>r._2._2)

    wordRDD.cache()

    val tagNumMap:Map[String,Int] = wordRDD.flatMap(r=>{
      r.map((_,1))
    }).reduceByKey(_ + _).collect.toMap

    val twoTagNumRdd:RDD[(String,String,Int,Int,Int)] = wordRDD.flatMap(r=>{
      // 求迪卡尔积,将任意两个tag组合在一起
      r.flatMap(w=>{
        r.filter(t=>t>w).map(t=>((t,w),1))
      })
    }).reduceByKey(_+_).map(r=>{
      (r._1._1,r._1._2,r._2)
    }).map(r=>{
      try{
        (r._1,r._2,tagNumMap(r._1),tagNumMap(r._2),r._3)
      }catch{
        case e:Exception =>(r._1,r._2,0,0,r._3)
      }
    })

    val st: StructType = DXPUtils.createSchema(Array(("tag1","string"),("tag2","string"),
      ("tag1num","int"),("tag2num","int"),("twotagnum","int")))
    val df: DataFrame = sparkEnv.hiveContext.createDataFrame(twoTagNumRdd.map(r => {
      Row(r._1,r._2,r._3,r._4,r._5)
    }), st)
    DXPUtils.saveDataFrame(df, dstTable, dt, sparkEnv.hiveContext)

  }

}
