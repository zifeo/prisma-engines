package queries.regressions

import org.scalatest.{FlatSpec, Matchers}
import util.ConnectorCapability.JoinRelationLinksCapability
import util._

class DateTimeFilterRegressionSpec extends FlatSpec with Matchers with ApiSpecBase with SchemaBaseV11 {
  // Validates fix for
  // - https://github.com/prisma/prisma/issues/3985
  // - https://github.com/prisma/prisma/issues/3987
  //
  // Summary:
  // A DateTime filter was incorrectly rendered as truthy (e.g. always 1=1) in the underlying query, causing it
  // to be irrelevant to the query.

  override def runOnlyForCapabilities: Set[ConnectorCapability] = Set(JoinRelationLinksCapability)

  "DateTime filters" should "not be ignored" in {
    val project = ProjectDsl.fromString {
      s"""
         |model ModelA {
         |  id Int      @id
         |  bs ModelB[]
         |}
         |
         |model ModelB {
         |  id     Int      @id
         |  date   DateTime
         |  fk     Int
         |  marker Boolean  @default(false)
         |  a      ModelA   @relation(fields: [fk], references: [id])
         |  
         |  @@unique([fk, date])
         |}
       """.stripMargin
    }
    database.setup(project)

    // Updating many B and filtering by date fails, date is ignored
    // fk id in [1, 2]
    // date on a specific date

    // Test data:
    // A1 -> B1 (date 1), B2 (date 2)
    // A2 -> B3 (date 2)
    // A3 -> No B
    server.query(
      s"""
         |mutation {
         |  createOneModelA(
         |    data: {
         |      id: 1
         |      bs: {
         |        create: [
         |          {
         |            id: 1,
         |            date: "2020-09-01T00:00:00.000Z",
         |          },
         |          {
         |            id: 2,
         |            date: "2020-11-09T10:56:30.000Z",
         |          }
         |        ]
         |      }
         |    }
         |  ){
         |    id
         |  }
         |}
       """.stripMargin,
      project,
      legacy = false
    )

    server.query(
      s"""
         |mutation {
         |  createOneModelA(
         |    data: {
         |      id: 2
         |      bs: {
         |        create: [
         |          {
         |            id: 3,
         |            date: "2020-11-09T10:56:30.000Z",
         |          }
         |        ]
         |      }
         |    }
         |  ){
         |    id
         |  }
         |}
       """.stripMargin,
      project,
      legacy = false
    )

    server.query(
      s"""
         |mutation {
         |  createOneModelA(
         |    data: {
         |      id: 3
         |    }
         |  ){
         |    id
         |  }
         |}
       """.stripMargin,
      project,
      legacy = false
    )

    // With a filter on:
    // - fk in 1, 2
    // - date on date 1
    // The only Record getting updated should be B1 (has A1, has date 1).
    val result = server.query(
      s"""
         |mutation {
         |  updateManyModelB(
         |    where: {
         |      fk: { in: [1, 2] }
         |      date: "2020-09-01T00:00:00.000Z",
         |    }
         |    data: {
         |      marker: true
         |  }) {
         |    count
         |  }
         |}
       """.stripMargin,
      project,
      legacy = false
    )

    result.toString() should be("""{"data":{"updateManyModelB":{"count":1}}}""")

    val check = server.query(
      s"""
         |{
         |  findOneModelB(where: { id: 1 }) {
         |    marker
         |  }
         |}
       """.stripMargin,
      project,
      legacy = false
    )

    check.toString() should be("""{"data":{"findOneModelB":{"marker":true}}}""")
  }
}
