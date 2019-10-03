package dynamite

import org.scalatest._

class LazySpec
  extends FlatSpec
  with Matchers
  with EitherValues {

  "lazy" should "not eval its argument until accessed" in {
    var test = false
    val l = Lazy(test = true)
    l.accessed
    test shouldBe false
  }

  it should "eval on first access" in {
    var count = 0
    val l = Lazy {
      count += 1
      count
    }
    val value = l()
    val second = l()
    count shouldBe 1
    value shouldBe 1
    second shouldBe 1
  }

  it should "indicate eval state with accessed" in {
    var count = 0
    val l = Lazy {
      count += 1
      count
    }
    l.accessed shouldBe false
    l()
    l.accessed shouldBe true
  }

  it should "update mapped value" in {
    var count = 0
    val l = Lazy {
      count += 1
      count
    }.map(_ + 1)
    val value = l()
    val second = l()
    count shouldBe 1
    value shouldBe 2
    second shouldBe 2
  }

}

