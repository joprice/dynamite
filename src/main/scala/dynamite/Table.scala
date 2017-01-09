package dynamite

import java.io.{ PrintWriter, StringWriter }
import fansi.{ Bold, Str }

object Table {
  def apply(
    headers: Seq[String],
    data: Seq[Seq[Str]],
    width: Option[Int]
  ): String = {
    val str = new StringWriter()
    val out = new PrintWriter(str)

    val rows = headers.map(header => Bold.On(Str(header))) +: data

    val colCount = headers.size
    val maxes = new Array[Int](colCount)
    val rowCount = rows.size

    (0 until rowCount).foreach { i =>
      (0 until colCount).foreach { j =>
        val k = rows(i)(j).length
        if (k > maxes(j)) {
          maxes(j) = k
        }
      }
    }

    rows.foreach { row =>
      val output = row.zipWithIndex.map {
        case (col, i) =>
          val max = maxes(i)
          val size = col.length
          val rendered = col.render
          if (size < max) rendered + (" " * (max - size)) else rendered
      }.mkString("\t")
      out.println(output)
    }

    str.toString
  }

}
