package scala_jsoup

import common.DXPUtils
import org.jsoup.Jsoup
import org.jsoup.nodes.{Document, Element}

import scala.collection.JavaConversions._

/**
  * Created by dxp on 2017/6/13.
  */
object FileHtmlParse {
  def main(args: Array[String]): Unit = {
    val lineArr: Array[String] = DXPUtils.tryOpenFile("data/xaasp20170612-69")
    lineArr.take(10).map(r => {
      val arr = r.split("\001")
      val htmlParser:Document = Jsoup.parse(arr(1))
      val doc:Element = htmlParser.body()

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
      val synoWords: Array[String] =
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

      val inputWord:String =
        try{
          doc.select("input#query")(0).attr("value")
        }catch {
          case e:Exception =>{
            e.printStackTrace()
            ""
          }
        }

      val dirWords:Array[String] =
        try{
          val dirEle = doc.select("div.lemmaWgt-lemmaCatalog")
          dirEle.select("a").filter(l=>{
            val href = l.attr("href")
            href.startsWith("#") && !href.contains("_")
          }).map(l=>l.text()).toArray
        }catch {
          case e:Exception => Array("")
        }
      //(arr(0), links.mkString(","), tags.mkString(","), synoWords.mkString(","))
      (arr(0),inputWord,dirWords.mkString(","))
    }).foreach(r => {
      println(Array(r._1,r._2,r._3).mkString("***"))
    })
  }

}
