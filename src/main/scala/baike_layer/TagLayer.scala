package baike_layer

import java.io.{File, PrintWriter}

import common.{DXPUtils, SparkEnv}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


/**
  * Created by duanxiping on 2017/2/6.
  * Updated: 2017-06-21 bug:层次对应的数字有问题,暂未解决
  */
object TagLayer {
  val srcTable = "algo.dxp_label_baike_words_two_tag_co_occur"
  val dstTable = "algo.dxp_label_baike_two_tag_layer"
  val sparkEnv = new SparkEnv("TwoTagLayer")

  def main(args: Array[String]): Unit = {
    val dt = args(0)
    val debug = args(1).toLowerCase == "true"
    val thres = args(2).toDouble

    val selectSQL = s"select tag1,tag2,tag1num,tag2num,twoTagNum from ${srcTable}"
    //设置过滤条件
    val tagRDD: RDD[(String, String)] = sparkEnv.hiveContext.sql(selectSQL).
      repartition(80).map(r => {
      val tag1: String = r.getAs[String](0)
      val tag2: String = r.getAs[String](1)
      val tagNum1: Int = r.getAs[Int](2)
      val tagNum2: Int = r.getAs[Int](3)
      val twoTagNum: Int = r.getAs[Int](4)
      (tag1, tag2, tagNum1, tagNum2, twoTagNum)
    }).filter(r => {
      r._5 > 10 && {
        val min: Int = if (r._3 > r._4) r._4 else r._3
        (min * thres).toInt < r._5
      }
    }).map(r => {
      if (r._3 > r._4)
        (r._2, r._1)
      else
        (r._1, r._2)
    })

    if (false) {
      val st: StructType = DXPUtils.createStringSchema(Array("ctag", "ptag"))
      val df: DataFrame = sparkEnv.hiveContext.createDataFrame(tagRDD.map(r => {
        Row(r._1, r._2)
      }), st)
      DXPUtils.saveDataFrame(df, dstTable, dt, sparkEnv.hiveContext)
    }
    // child->parent
    var c2pArr = tagRDD.collect()
    sparkEnv.sc.stop()

    val cw = new PrintWriter(new File("ori.txt"))
    for (elem <- c2pArr) {
      cw.write(elem._1 + ">" + elem._2 + "\n")
    }
    cw.close()

    if (debug) {
      println("*" * 10 + "c2pArr" + "*" * 10)
      c2pArr.foreach(r => {
        println(r._1 + "->" + r._2)
      })
    }


    var flag: Boolean = true
    val arrBuf = new ArrayBuffer[(String, String)]()
    val layerMap = new mutable.HashMap[Int, Array[String]]()
    var i = 0

    while (flag) {
      val (singArr, resArr) = getSingleParent(c2pArr)
      //top tag
      if (i == 0) {
        layerMap.put(i, singArr.map(_._2).distinct)
      }
      //no top tag
      i = i + 1
      layerMap.put(i, singArr.map(_._1))

      arrBuf.appendAll(singArr)
      if (resArr.length == 0) {
        flag = false
      }
      c2pArr = resArr
      println(s"cycle time:${i}")
    }
    val sb: StringBuffer = new StringBuffer()
    arrBuf.foreach(r => {
      sb.append(r._1 + ">" + r._2 + "\n")
    })
    val writer = new PrintWriter(new File("res.txt"))
    writer.write(sb.toString)
    writer.close()

    val layw = new PrintWriter(new File("layer.txt"))
    val tags = layerMap.toArray.flatMap(r => {
      r._2.map(w => (w, r._1))
    }).toMap

    tags.foreach(r => {
      layw.write(r._1 + " " + r._2 + "\n")
    })
    layw.close()

    //获得层次关系
    val aBuf = new ArrayBuffer[Array[String]]()
    tags.foreach(r => {
      var tag = r._1
      val tagBuff = new ArrayBuffer[String]
      var flag = true
      while (flag) {
        tagBuff.append(tag)
        //取出父tag,当tag是根结点,引发异常,可以退出while循环
        try {
          tag = arrBuf.filter(_._1 == tag)(0)._2
        } catch {
          case e: Exception => flag = false
        }
      }
      aBuf.append(tagBuff.toArray)
    })

    val multiLayw = new PrintWriter(new File("multilayer.txt"))
    aBuf.foreach(r=>{
      multiLayw.write(r.reverse.mkString(">")+"\n")
    })
    multiLayw.close()



    /*
      if(debug) {
        println("*"*10+"singleArr" + "*"*10)
        singArr.foreach(r=>{
          println(r._1+ "->"+r._2)
        })
        println("*" * 10 + "resArr" + "*" * 10)
        resArr.foreach(r => {
          println(r._1 + "->" + r._2)
        })
      }
     */
  }


  /*去掉越级标签, 保证每个标签只有一个父标签
  1. 找到顶级标签 如 A->None,即A
  2. 找到顶级标签的子标签 B->A C-> D->A E->A,即B,C,D,E
  3. 去掉子标签的子标签 C->B F->B 即 C,F
  4. 去掉越级标签 C,剩下B,D,E
   */
  def getSingleParent(arr: Array[(String, String)]):
  (Array[(String, String)], Array[(String, String)]) = {
    val parentArr = arr.map(_._2)
    val childArr = arr.map(_._1)

    val topParentArr: Array[String] = parentArr.filterNot(childArr.toSet.contains(_))
    //得到顶级标签
    val topChildArr: Array[String] = arr.filter(r => topParentArr.contains(r._2)).map(_._1)
    //顶级标签的子标签
    val tmpChildArr: Array[String] = arr.filter(r => topChildArr.contains(r._2)).map(_._1)
    //顶级标签的子标签的子标签
    val firstParentArr: Array[String] = topChildArr.filterNot(tmpChildArr.toSet.contains(_))
    //去掉越级标签
    val singleArr: Array[(String, String)] = firstParentArr.flatMap(r => {
      topParentArr.map(w => (r, w)).filter(t => arr.contains(t))
    }).distinct
    val resArr = arr.filterNot(r => topParentArr.contains(r._2))
    (singleArr, resArr)
  }

}
