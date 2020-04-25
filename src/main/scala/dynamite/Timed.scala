package dynamite

import scala.concurrent.duration._
import zio.ZIO
import zio.clock.{nanoTime, Clock}

final case class Timed[A] private (result: A, duration: FiniteDuration) {
  def map[B](f: A => B): Timed[B] = Timed(f(result), duration)
}

object Timed {
  def apply[R, E, A](effect: ZIO[R, E, A]): ZIO[R with Clock, E, Timed[A]] = {
    for {
      before <- nanoTime
      result <- effect
      after <- nanoTime
    } yield Timed(result, (after - before).nanos)
  }
}
