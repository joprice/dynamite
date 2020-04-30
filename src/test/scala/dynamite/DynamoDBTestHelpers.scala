package dynamite

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType
import dynamite.Dynamo.DynamoClient
import dynamite.ScriptSpec.createTable
import zio.{Task, ZIO, ZLayer, ZManaged}

object DynamoDBTestHelpers {
  def withTable[T](tableName: String)(
      attributeDefinitions: (String, ScalarAttributeType)*
  )(
      client: AmazonDynamoDB
  ) =
    ZManaged.make {
      createTable(client)(tableName)(attributeDefinitions: _*)
    }(_ => Task(client.deleteTable(tableName)).orDie)

  val dynamoClient =
    ZLayer.fromManaged(
      ZManaged
        .fromEffect(DynamoDBLocal.randomPort)
        .flatMap(port =>
          DynamoDBLocal.dynamoServer(port) *> dynamoLocalClient(port)
        )
    )

  def dynamoLocalClient(port: Int) =
    ZManaged.makeEffect(
      Dynamo
        .dynamoClient(
          endpoint = Some(s"http://localhost:$port"),
          credentials = Some(
            new AWSStaticCredentialsProvider(new BasicAWSCredentials("", ""))
          )
        )
    )(_.shutdown())

  def cleanupTable(tableName: String) =
    ZIO.accessM[DynamoClient] { value =>
      Task(value.get.deleteTable(tableName)).orDie
    }

}
