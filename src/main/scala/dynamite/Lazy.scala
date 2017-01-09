package dynamite

class Lazy[+A](value: => A) {
  private[this] var _accessed = false
  def accessed = _accessed
  private[this] lazy val _value = {
    _accessed = true
    value
  }
  def apply() = _value
  def isEmpty = false
  def get = _value

  def map[B](f: A => B): Lazy[B] = new Lazy(f(apply()))
}

object Lazy {
  def apply[A](value: => A) = new Lazy(value)

  def unapply[A](value: Lazy[A]) = value
}
