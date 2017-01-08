package dynamite

class Lazy[A](value: => A) {
  private[this] var _accessed = false
  def accessed = _accessed
  private[this] lazy val _value = {
    _accessed = true
    value
  }
  def apply() = _value
}

object Lazy {
  def apply[A](value: => A) = new Lazy(value)
}
