package dynamite

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter

/**
 * A pretty printer that removes the space before the colon between keys and values:  '{ "a": 1 }'
 */
class CustomPrettyPrinter extends DefaultPrettyPrinter {

  // This has to be overridden. Otherwise, the superclass implementation will be used when this is called internally
  override def createInstance(): CustomPrettyPrinter = new CustomPrettyPrinter

  override def writeObjectFieldValueSeparator(jg: JsonGenerator): Unit = {
    jg.writeRaw(": ")
  }
}
