package dynamite

import scala.concurrent.duration._

final case class Timed[A] private (result: A, duration: FiniteDuration)

object Timed {
  def apply[A](f: => A): Timed[A] = {
    val before = System.nanoTime
    val result = f
    val after = System.nanoTime
    Timed(result, (after - before).nanos)
  }
}
