package dynamite

import dynamite.Ast.{Query, ReplCommand}
import com.amazonaws.services.dynamodbv2.model.{
  ResourceNotFoundException,
  ScalarAttributeType
}
import dynamite.Dynamo.{Dynamo, DynamoClient}
import dynamite.Eval.{
  AmbiguousIndexException,
  InvalidHashKeyException,
  InvalidRangeKeyException,
  MissingRangeKeyException,
  UnexpectedRangeKeyException,
  UnknownIndexException,
  UnknownTableException
}
import dynamite.Response.{Index, KeySchema, ResultSet, TableDescription}
import DynamoDBTestHelpers._
import play.api.libs.json.{JsValue, Json}
import zio.random.Random
import zio.stream.ZStream
import zio.{Chunk, Has, Task, ZIO, ZManaged}
import zio.test._
import zio.test.Assertion._

object EvalSpec extends DefaultRunnableSpec {
  val tableName = "eval-spec"
  val rangeKeyTableName = "eval-spec-range-key"

  case object CreateTables
  type ClientInit = Has[CreateTables.type]

  val createTables =
    ZManaged
      .access[DynamoClient](_.get)
      .flatMap { client =>
        ZManaged.makeEffect(
          Seed.createTableWithoutHashKey(rangeKeyTableName, client)
        )(_ => client.deleteTable(rangeKeyTableName)) *>
          ZManaged
            .makeEffect(Seed.createTable(tableName, client))(_ =>
              client.deleteTable(tableName)
            )
            .as(client)
            .tapM(client => Task(Seed.insertSeedData(tableName, client)))
      }

  def run(
      query: String
  ): ZIO[Random with DynamoClient with Dynamo, Throwable, Response] =
    ZManaged
      .access[DynamoClient](_.get)
      .use { client =>
        val tableCache = new TableCache(Eval.describeTable)
        ZIO.fromEither(Parser.parse(query)).flatMap {
          case result: Query => Eval(result, tableCache, pageSize = 20)
          case _: ReplCommand =>
            Task.fail(new Exception("Invalid command type"))
        }
      }

  //TODO: restructure test to use Access for client/table, to avoid recreating table
  def validate(
      query: String,
      expected: Chunk[List[JsValue]]
  ): ZIO[Random with DynamoClient with Eval.Env, Throwable, TestResult] =
    run(query)
      .flatMap {
        case Response.ResultSet(pages, _) =>
          pages
            .mapM { page =>
              Task.foreach(page.result.data) { json =>
                Script.dynamoObjectToJson(json)
              }
            }
            .runCollect
            .map {
              assert(_)(equalTo(expected))
            }
        case _ => Task.fail(new Exception("Invalid result"))
      }

  def spec =
    suite("eval")(
      testM("select all records from dynamo") {
        validate(
          s"select id, name from $tableName",
          Chunk(
            List(
              Json.obj("id" -> 1, "name" -> "Chill Times"),
              Json.obj("id" -> 2, "name" -> "EDM4LYFE"),
              Json.obj("id" -> 3, "name" -> "Disco Fever"),
              Json.obj("id" -> 4, "name" -> "Top Pop")
            )
          )
        )
      },
      testM("create table") {
        (for {
          _ <- run(query = s"create table users (userId string)")
          _ <- run(query = s"create table products (userId string)")
          tables <- ZStream.accessStream[Dynamo](_.get.listTables).runCollect
        } yield {
          assert(tables)(
            equalTo(
              Chunk(
                List(
                  "eval-spec",
                  "eval-spec-range-key",
                  "products",
                  "users"
                )
              )
            )
          )
        }).ensuring(cleanupTable("users"))
          .ensuring(cleanupTable("products"))
      },
      testM("create table if not exists") {
        (for {
          _ <- run(query = s"create table users (userId string)")
          _ <- run(query = s"create table if not exists users (userId string)")
          tables <- ZStream.accessStream[Dynamo](_.get.listTables).runCollect
        } yield {
          assert(tables)(
            equalTo(
              Chunk(
                List(
                  "eval-spec",
                  "eval-spec-range-key",
                  "users"
                )
              )
            )
          )
        }).ensuring(cleanupTable("users"))
      },
      testM("allow selecting all fields") {
        validate(s"select * from $tableName", Chunk(Seed.seedData))
      },
      testM("allow explicit ascending order") {
        validate(
          s"select * from $tableName where userId = 'user-id-1' order by id asc",
          Chunk(
            Seed.seedData
              .filter(json => (json \ "userId").as[String] == "user-id-1")
          )
        )
      },
      testM("check key type") {
        assertM(
          run(s"select * from $tableName where userId = 456").flip
            .map(_.getMessage)
        )(equalTo("Wrong type for key userId. Expected string, found number"))
      },
      testM("check key type on insert") {
        assertM(
          run(s"insert into $tableName (userId) values(456)").flip
            .map(_.getMessage)
        )(equalTo("Wrong type for key userId. Expected string, found number"))
      },
      testM("check key type on update") {
        assertM(
          run(s"update $tableName set id = 3 where userId = 1").flip
            .map(_.getMessage)
        )(equalTo("Wrong type for key userId. Expected string, found number"))
      },
      testM("check key type on delete") {
        assertM(
          run(s"delete from $tableName where userId = 1").flip
            .map(_.getMessage)
        )(equalTo("Wrong type for key userId. Expected string, found number"))
      },
      testM("allow reversing the results ") {
        validate(
          s"select * from $tableName where userId = 'user-id-1' order by id desc",
          Chunk(
            Seed.seedData
              .filter(json => (json \ "userId").as[String] == "user-id-1")
              .reverse
          )
        )
      },
      // TODO: warn when fields are not present in type
      // TODO: disallow asc/desc on scan?
      // TODO: nested fields
      // TODO: catch com.amazonaws.AmazonServiceException where message contains 'Query condition missed key schema element'
      testM("select all records for a given user") {
        validate(
          s"select name from $tableName where userId = 'user-id-1'",
          Chunk(
            List(
              Json.obj("name" -> "Chill Times"),
              Json.obj("name" -> "EDM4LYFE")
            )
          )
        )
      },
      testM("limit results") {
        validate(
          s"select name from $tableName where userId = 'user-id-1' limit 1",
          Chunk(
            List(
              Json.obj("name" -> "Chill Times")
            )
          )
        )
      },
      testM("support updating a field") {
        val newName = "Chill Timez"
        run(
          s"update $tableName set name = '$newName' where userId = 'user-id-1' and id = 1"
        ) *>
          validate(
            s"select name from $tableName where userId = 'user-id-1' and id = 1",
            Chunk(
              List(
                Json.obj("name" -> newName)
              )
            )
          )
      },
      testM("support updating an object field") {
        val first = Seed.seedData.head
        val userId = (first \ "userId").as[String]
        val id = (first \ "id").as[Int]
        run(
          s"""update $tableName set meta = {
          "tags": ["rock", "metal"],
          "visibility": "private"
          } where userId = '$userId' and id = $id"""
        ) *>
          validate(
            s"select * from $tableName where userId = '$userId' and id = $id",
            Chunk(
              List(
                first ++ Json.obj(
                  "meta" -> Json.obj(
                    "tags" -> List("rock", "metal"),
                    "visibility" -> "private"
                  )
                )
              )
            )
          )
      },
      testM("support updating without range key") {
        val newName = "new title"
        val userId = "user-id-1"
        run(
          s"""insert into $rangeKeyTableName (userId, name) values ('$userId', "original-title")"""
        ) *>
          run(
            s"update $rangeKeyTableName set name = '$newName' where userId = '$userId'"
          ) *>
          validate(
            s"select name from $rangeKeyTableName where userId = '$userId'",
            Chunk(
              List(
                Json.obj("name" -> newName)
              )
            )
          )
      },
      testM("support deleting a record") {
        run(
          s"delete from $tableName where userId = 'user-id-1' and id = 1"
        ) *>
          validate(
            s"select name from $tableName where userId = 'user-id-1' and id = 1",
            Chunk(List())
          )
      },
      testM("support inserting a record") {
        val newName = "Throwback Thursday"
        run(
          s"""insert into $tableName (userId, id, name, tracks) values ('user-id-1', 20, "$newName", [1,2,3])"""
        ) *>
          validate(
            s"select name from $tableName where userId = 'user-id-1' and id = 20",
            Chunk(
              List(
                Json.obj("name" -> newName)
              )
            )
          )
      },
      testM("support float values") {
        val newName = "Throwback Thursday"
        run(
          s"""insert into $tableName (userId, id, name, duration) values ('user-id-1', 20, "$newName", 1.1)"""
        ) *>
          validate(
            s"select duration from $tableName where userId = 'user-id-1' and id = 20",
            Chunk(
              List(
                Json.obj("duration" -> 1.1)
              )
            )
          )
      },
      // TODO: this could use playlist-name or playlist-name-global
      testM("use an index when unambiguous") {
        validate(
          s"select id from $tableName where userId = 'user-id-1' and name = 'EDM4LYFE'",
          Chunk(
            List(
              Json.obj("id" -> 2)
            )
          )
        )
      },
      testM("fail when table is unknown") {
        assertM(
          run(
            s"select id from unknown-table where userId = 'user-id-1' and name = 'EDM4LYFE'"
          ).flip
        )(isSubtype[UnknownTableException](anything))
      },
      testM("return ambiguous index error") {
        assertM(
          run(
            s"select id from $tableName where userId = 'user-id-1' and duration = 10"
          ).flip
        )(isSubtype[AmbiguousIndexException](anything))
      },
      testM("use an explicit index when provided") {
        validate(
          s"select id from $tableName where userId = 'user-id-1' and duration = 10 use index playlist-length-keys-only",
          Chunk(
            List(
              Json.obj("id" -> 2),
              Json.obj("id" -> 1)
            )
          )
        )
      },
      testM("fail when invalid range key is used with explicit index") {
        val index = "playlist-length-keys-only"
        assertM(
          run(
            s"select id from $tableName where userId = 'user-id-1' and id = 1 use index $index"
          ).flip
        )(
          equalTo(
            InvalidRangeKeyException(
              index,
              KeySchema("duration", ScalarAttributeType.N),
              "id"
            )
          )
        )
      },
      testM(
        "fail when range key is provided for an explicit index that doesn't have a range key"
      ) {
        val index = "playlist-name-global"
        assertM(
          run(
            s"select id from $tableName where name = 'name' and id = 1 use index $index"
          ).flip
        )(equalTo(UnexpectedRangeKeyException("id")))
      },
      testM(
        "fail when range key is not provided for an explicit index that does have a range key"
      ) {
        val index = "playlist-length-keys-only"
        assertM(
          run(
            s"select id from $tableName where userId = 'user-id-1' use index $index"
          ).flip
        )(equalTo(MissingRangeKeyException("duration")))
      },
      testM("fail when index does not exist with hash provided") {
        assertM(
          run(
            s"select id from $tableName where userId = 'user-id-1' use index fake-index"
          ).flip
        )(equalTo(UnknownIndexException("fake-index")))
      },
      testM("fail when index does not exist with hash and range provided") {
        assertM(
          run(
            s"select id from $tableName where userId = 'user-id-1' and duration = 10 use index fake-index"
          ).flip
        )(equalTo(UnknownIndexException("fake-index")))
      },
      testM("fail when hash key does not match index hash key") {
        assertM(
          run(
            s"select id from $tableName where name = 'Disco Fever' use index playlist-name"
          ).flip
        )(
          equalTo(
            InvalidHashKeyException(
              Index(
                "playlist-name",
                KeySchema("userId", ScalarAttributeType.S),
                Some(KeySchema("name", ScalarAttributeType.S))
              ),
              "name"
            )
          )
        )
      },
      testM("query against an explicit index") {
        validate(
          s"select id from $tableName where name = 'Disco Fever' use index playlist-name-global",
          Chunk(
            List(
              Json.obj("id" -> 3)
            )
          )
        )
      },
      testM("query against an index when unambiguous") {
        validate(
          s"select id from $tableName where name = 'Disco Fever'",
          Chunk(
            List(
              Json.obj("id" -> 3)
            )
          )
        )
      },
      testM("aggregate supports count") {
        validate(
          s"select count(*) from $tableName",
          Chunk(
            List(
              Json.obj("count" -> 4)
            )
          )
        )
      },
      testM("support count with filters") {
        validate(
          s"select count(*) from $tableName where userId = 'user-id-1'",
          Chunk(
            List(
              Json.obj("count" -> 2)
            )
          )
        )
      },
      testM("fail on count with column name") {
        assertM(
          run(s"select count(id) from $tableName where userId = 'user-id-1'").flip
            .map(_.getMessage)
        )(equalTo("Failed parsing query"))
      },
      testM("support count with other fields") {
        assertM(
          run(s"select count(*), name from $tableName").flip.map(_.getMessage)
        )(
          equalTo(
            "Aggregates may not be used with unaggregated fields"
          )
        )
      },
      //TODO: validate that keys match table
//      testM("validate key schema") {
//        assertM(
//          run(
//            s"select count(*) from $tableName where userid = 'user-id-1'"
//          ).flatMap {
//            case ResultSet(_, capacity) => Task.succeed(capacity)
//            case _                      => Task.fail(new Exception("Unexpected result"))
//          }
//        )(isSome(anything))
//      },
      testM("return consumed capacity") {
        assertM(
          run(
            s"select count(*) from $tableName where userId = 'user-id-1'"
          ).flatMap {
            case ResultSet(_, capacity) => capacity
            case _                      => Task.fail(new Exception("Unexpected result"))
          }
        )(isSome(anything))
      },
      testM("return table description") {
        run(
          s"describe table $tableName"
        ).flatMap {
            case table: TableDescription => Task.succeed(table)
            case _                       => Task.fail(new Exception("Unexpected result"))
          }
          .map {
            table =>
              assert(table)(
                equalTo(
                  TableDescription(
                    "eval-spec",
                    KeySchema("userId", ScalarAttributeType.S),
                    Some(KeySchema("id", ScalarAttributeType.N)),
                    Seq(
                      Index(
                        "playlist-name-global",
                        KeySchema("name", ScalarAttributeType.S),
                        None
                      ),
                      Index(
                        "playlist-length",
                        KeySchema("userId", ScalarAttributeType.S),
                        Some(KeySchema("duration", ScalarAttributeType.N))
                      ),
                      Index(
                        "playlist-length-keys-only",
                        KeySchema("userId", ScalarAttributeType.S),
                        Some(KeySchema("duration", ScalarAttributeType.N))
                      ),
                      Index(
                        "playlist-name",
                        KeySchema("userId", ScalarAttributeType.S),
                        Some(KeySchema("name", ScalarAttributeType.S))
                      )
                    )
                  )
                )
              )
          }
      },
      testM("render an error when table does not exist") {
        assertM(run(s"describe table non-existent-table"))(
          equalTo(Response.Info("No table exists with name non-existent-table"))
        )
      },
      testM("bubble up loading error")(
        assertM(
          Eval
            .assumeIndex(
              tableCache =
                new TableCache(_ => Task.fail(new Exception("fail"))),
              tableName = "users",
              hashKey = "userId",
              rangeKey = Some("id"),
              orderBy = None
            )
            .flip
            .map(_.getMessage)
        )(equalTo("fail"))
      ),
      testM("tolerate missing table")(
        assertM(
          Eval
            .assumeIndex(
              tableCache = new TableCache(_ =>
                Task.fail(new ResourceNotFoundException("unknown table"))
              ),
              tableName = "users",
              hashKey = "userId",
              rangeKey = Some("id"),
              orderBy = None
            )
            .flip
        )(equalTo(UnknownTableException("users")))
      )
    ) @@ TestAspect.sequential @@ TestAspect.aroundTest(
      createTables
        .map(_ => ZIO.succeed(_: TestSuccess))
        .mapError(TestFailure.die)
    ) provideCustomLayerShared ((dynamoClient >>> Dynamo.live.passthrough).orDie)
}
