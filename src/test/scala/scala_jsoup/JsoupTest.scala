package scala_jsoup

import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.jsoup.nodes.Element

/**
  * Created by dxp on 2017/6/12.
  */
object JsoupTest{
  def main(args: Array[String]) {
    /*
    val html = "<p>An <a href='http://example.com/'><b>example</b></a> link.</p>"
    val doc = Jsoup.parse(html)
    //解析HTML字符串返回一个Document实现
    val link = doc.select("a").first
    //查找第一个a元素
    val text = doc.body.text // "An example link"//取得字符串中的文本
    System.out.println(text)
    val linkHref = link.attr("href")
    // "http://example.com/"//取得链接地址
    val linkText = link.text // "example""//取得链接地址中的文本
    System.out.println("linkHref:" + linkHref)
    System.out.println("LinkText:" + linkText)
    val linkOuterH = link.outerHtml
    // "<a href="http://example.com"><b>example</b></a>"
    val linkInnerH = link.html // "<b>example</b>"//取得链接内的html内容
    System.out.println("linkOuterH:" + linkOuterH)
    System.out.println("linkInnerH:" + linkInnerH)
    */

    val html1 =
      """
        |<meta name="keywords" content="中国 古代中国 中国词义 中国历史 中国地理 中国政治 中国军事 中国经济 中国文化 中国科技 中国社会">
        |<meta name="image" content="http://baike.bdimg.com/cms/static/baike.png">
      """.stripMargin
    println(html1)

    val doc = Jsoup.parse(html1)
    val meta1 = doc.select("meta").first()
    println("name:" + meta1.attr("name"))
    println("content:" + meta1.attr("content"))
  }
}