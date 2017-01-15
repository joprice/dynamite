package dynamite

import com.amazonaws.services.dynamodbv2.document.DynamoDB
import org.scalatest._
import scala.concurrent.duration._

trait DynamoTestClient {
  val dynamoPortKey = "dynamodb.local.port"

  val dynamoPort = sys.props.get(dynamoPortKey).getOrElse {
    throw new Exception(s"Failed to find $dynamoPortKey")
  }

  lazy val client = Repl.dynamoClient(Some(s"http://localhost:$dynamoPort"))
}

trait DynamoSpec
    extends BeforeAndAfterAll
    with BeforeAndAfterEach
    with DynamoTestClient { self: Suite =>

  def tableNames: Seq[String]

  lazy val dynamo = new DynamoDB(client)

  override def afterEach() = {
    super.afterEach()
    tableNames.foreach { tableName =>
      val table = dynamo.getTable(tableName)
      table.delete()
      table.waitForDelete()
    }
  }

  override def afterAll() = {
    super.afterAll()
    client.shutdown()
  }

}
