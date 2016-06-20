package dynamite

class Lazy[A](value: => A) {
  var accessed = false
  private[this] lazy val _value = {
    accessed = true
    value
  }
  def apply() = _value
}

object Lazy {
  def apply[A](value: => A) = new Lazy(value)
}
