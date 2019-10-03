package dynamite

import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest
import com.amazonaws.services.dynamodbv2.util.TableUtils
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import org.scalatest._
import scala.concurrent.duration._
import com.amazonaws.auth.{ BasicAWSCredentials, AWSStaticCredentialsProvider }

trait DynamoTestClient {
  val dynamoPortKey = "dynamodb.local.port"

  val dynamoPort = sys.props.get(dynamoPortKey).getOrElse {
    throw new Exception(s"Failed to find $dynamoPortKey")
  }

  val credentials = new AWSStaticCredentialsProvider(new BasicAWSCredentials("", ""))
  lazy val client = Repl.dynamoClient(Some(s"http://127.0.0.1:$dynamoPort"), Some(credentials))
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
      TableUtils.deleteTableIfExists(client, new DeleteTableRequest().withTableName(tableName))
    }
  }

  override def afterAll() = {
    super.afterAll()
    client.shutdown()
  }

}
