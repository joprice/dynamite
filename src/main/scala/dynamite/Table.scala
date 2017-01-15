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
      val output = row.zipWithIndex.flatMap {
        case (col, i) =>
          val max = maxes(i)
          val size = col.length
          val rendered = col.render
          // avoid printing trailing spaces when the final column is empty
          val isLast = i == row.size - 1
          if (isLast && rendered.isEmpty) {
            None
          } else {
            Some(if (size < max && i < row.size - 1) rendered + (" " * (max - size)) else rendered)
          }
      }.mkString(" " * 3)
      out.println(output)
    }

    str.toString
  }

}
