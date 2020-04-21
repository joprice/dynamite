package dynamite

import zio.test._
import zio.test.Assertion._

object LazySpec extends DefaultRunnableSpec {

  def spec = suite("Lazy")(
    test("not eval its argument until accessed") {
      var test = false
      val l = Lazy { test = true }
      l.accessed
      assert(test)(isFalse)
    },
    test("eval on first access") {
      var count = 0
      val l = Lazy {
        count += 1
        count
      }
      val value = l()
      val second = l()
      assert(count)(equalTo(1)) &&
      assert(value)(equalTo(1)) &&
      assert(second)(equalTo(1))
    },
    test("indicate eval state with accessed") {
      var count = 0
      val l = Lazy {
        count += 1
        count
      }
      val before = l.accessed
      l()
      val after = l.accessed
      assert(before)(isFalse) &&
      assert(after)(isTrue)
    },
    test("update mapped value") {
      var count = 0
      val l = Lazy {
        count += 1
        count
      }.map(_ + 1)
      val value = l()
      val second = l()
      assert(count)(equalTo(1)) &&
      assert(value)(equalTo(2)) &&
      assert(second)(equalTo(2))
    }
  )
}
