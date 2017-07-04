package baike_layer

import common.{DXPUtils, SparkEnv}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}


/**
  * Created by duanxiping on 2017/2/6.
  */
object ClassifyRelWord {
  val srcTable = "algo.dxp_label_baike_word_relwords_tags"
  val dstTable = "algo.dxp_label_baike_words_tag_reltagwords"
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
    //加载指定的tag
    val selectedTags: Map[String, Int] = sparkEnv.sc.textFile("/tmp/duanxiping/bk_useful_tags.txt").map(r => {
      val arr = r.trim.split(" ")
      if (arr.length == 2)
        (arr(0), arr(1).toInt)
      else
        ("", -1)
    }).filter(_._2 != -1).collect.toMap
    val brSelectedTags = sparkEnv.sc.broadcast(selectedTags)

    val selectSQL = s"select bk_word,bk_relwords,bk_tags from ${srcTable}"
    //设置过滤条件
    val wordRDD: RDD[(String, Array[String], Int)] = sparkEnv.hiveContext.sql(selectSQL).
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
    }).filter(r => {
      //过滤出含有指定tag的词条
      r._2._2.filter(brSelectedTags.value.contains(_)).length > 0
    }).map(r => {
      // 计算词所属的类别 0:人物 1:电影电视作品 2:组织机构等
      val tagsMap = brSelectedTags.value
      val mapData = scala.collection.mutable.Map[Int, Int]()
      r._2._2.map(w => {
        try {
          tagsMap(w)
        } catch {
          case e: Exception => -1
        }
      }).filter(_ != -1).foreach(x => {
        mapData.get(x) match {
          case Some(b) => mapData += (x -> b.+(1))
          case None => mapData += (x -> 1)
        }
      })
      val tagClass:Int = mapData.toArray.sortWith(_._2 > _._2)(0)._1
      (r._1, r._2._1, tagClass)
    })

    wordRDD.cache()

    if (debug) {
      val wordTable = "algo.dxp_label_baike_cleaned_words"
      val st: StructType = DXPUtils.createStringSchema(Array("bk_word", "bk_relwords", "bk_tag"))
      val df: DataFrame = sparkEnv.hiveContext.createDataFrame(wordRDD.map(r => {
        Row(r._1, r._2.mkString(","), r._3.toString)
      }), st)
      DXPUtils.saveDataFrame(df, wordTable, dt, sparkEnv.hiveContext)
    }

    val wordTags:Map[String,Int] = wordRDD.map(r => {
      (r._1, r._3)
    }).collect().toMap
    println("*"*20 + "wordTags")

    val brWordTags = sparkEnv.sc.broadcast(wordTags)
    val resRDD:RDD[(String,Int,Int,Array[(String,Int)])] = wordRDD.flatMap(r=>{
      val word:String = r._1
      val wordClass:Int = r._3
      r._2.sortWith(_ > _).map(w =>{
        val relWordClass =
        try{
          brWordTags.value(w)
        }catch {
          case e:Exception => -1
        }
        ((word,wordClass,relWordClass),Array(w))
      })
    }).reduceByKey(_++_).map(r=>{
      val relArr:Array[(String,Int)] = r._2.groupBy(w=>w).toArray.map(w=>{
        (w._1,w._2.length)
      }).sortWith(_._2>_._2)
      (r._1._1,r._1._2,r._1._3,relArr)
    })

    val st: StructType = DXPUtils.createSchema(Array(("bk_word","string"),("bk_tag","int"),
      ("bk_reltag","int"),("bk_relwords","string")))
    val df: DataFrame = sparkEnv.hiveContext.createDataFrame(resRDD.map(r => {
      Row(r._1,r._2,r._3,r._4.map(w=>w._1+"/" + w._2).mkString(","))
    }), st)
    DXPUtils.saveDataFrame(df, dstTable, dt, sparkEnv.hiveContext)
  }

}
