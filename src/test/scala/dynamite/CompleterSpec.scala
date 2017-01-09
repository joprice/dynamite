package dynamite

import java.io.{ ByteArrayOutputStream, File }
import java.nio.charset.StandardCharsets

import org.scalatest._

class CompleterSpec
    extends FlatSpec
    with Matchers
    with OptionValues {

  "table parser" should "parse a projection" in {
    Completer.tableParser.parse("select * from tableName") should matchPattern {
      case fastparse.core.Parsed.Success("tableName", _) =>
    }
  }

}

