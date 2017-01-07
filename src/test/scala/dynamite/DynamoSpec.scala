package dynamite

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import org.scalatest._
import scala.concurrent.duration._

trait DynamoTestClient {
  val dynamoPortKey = "dynamodb.local.port"

  val dynamoPort = sys.props.get(dynamoPortKey).getOrElse {
    throw new Exception(s"Failed to find $dynamoPortKey")
  }

  lazy val config = new ClientConfiguration()
    .withConnectionTimeout(1.second.toMillis.toInt)
    .withSocketTimeout(1.second.toMillis.toInt)
  lazy val client = new AmazonDynamoDBClient(new BasicAWSCredentials("", ""), config)
    .withEndpoint[AmazonDynamoDBClient](s"http://localhost:$dynamoPort")
}

trait DynamoSpec
    extends BeforeAndAfterAll
    with BeforeAndAfterEach
    with DynamoTestClient { self: Suite =>

  def tableName: String

  lazy val dynamo = new DynamoDB(client)
  lazy val table = dynamo.getTable(tableName)

  override def beforeEach() = {
    super.beforeEach()
    Seed(tableName, client)
  }

  override def afterEach() = {
    super.afterEach()
    table.delete()
    table.waitForDelete()
  }

  override def afterAll() = {
    super.afterAll()
    client.shutdown()
  }

}
