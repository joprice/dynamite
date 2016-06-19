package dynamite

import java.io.{ PrintWriter, StringWriter }

import fansi.{ Bold, Color, Str }

object Table {
  def apply(headers: Seq[String], data: Seq[Seq[Str]]): String = {
    val str = new StringWriter()
    val out = new PrintWriter(str)
    apply(
      out,
      headers.map(header => Bold.On(Str(header))) +: data
    )
    str.toString
  }

  def apply(out: PrintWriter, rows: Seq[Seq[Str]]): Unit = {
    val colCount = rows.head.size
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
      row.zipWithIndex.foreach {
        case (col, i) =>
          val max = maxes(i)
          val size = col.length
          val padded = if (size < max) col + (" " * (max - size)) else col
          out.print(padded)
          out.print("\t")
      }
      out.println
    }
  }
}
