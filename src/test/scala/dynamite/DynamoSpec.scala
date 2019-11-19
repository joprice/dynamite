package dynamite

import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest
import com.amazonaws.services.dynamodbv2.util.TableUtils
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import org.scalatest._
import scala.concurrent.duration._
import com.amazonaws.auth.{ BasicAWSCredentials, AWSStaticCredentialsProvider }
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded

trait DynamoTestClient {
  lazy val client = DynamoDBEmbedded.create().amazonDynamoDB()
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
