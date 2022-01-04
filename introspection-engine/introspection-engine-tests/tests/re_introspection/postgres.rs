use expect_test::expect;
use indoc::indoc;
use introspection_engine_tests::test_api::*;
use test_macros::test_connector;

#[test_connector(tags(Postgres))]
async fn mapped_relation_fields(api: &TestApi) -> TestResult {
    let ddl = indoc! {r#"
        CREATE TABLE "A" (
            "id" INTEGER NOT NULL,
            CONSTRAINT "A_pkey" PRIMARY KEY ("id")
        );

        CREATE TABLE "B" (
            "id" INTEGER NOT NULL,
            CONSTRAINT "B_pkey" PRIMARY KEY ("id")
        );

        ALTER TABLE "B" ADD CONSTRAINT "first_fk"
            FOREIGN KEY ("id") REFERENCES "A"("id")
            ON DELETE RESTRICT ON UPDATE CASCADE;

        ALTER TABLE "B" ADD CONSTRAINT "second_fk"
            FOREIGN KEY ("id") REFERENCES "A"("id")
            ON DELETE RESTRICT ON UPDATE CASCADE;
    "#};

    api.raw_cmd(ddl).await;

    let input_dm = indoc! {r#"
        model A {
          id Int @id
          b  B?  @relation("first")
          bs B?  @relation("second")
        }

        model B {
          id Int @id
          a1 A   @relation("first", fields: [id], references: [id], map: "first_fk")
          a2 A   @relation("second", fields: [id], references: [id], map: "second_fk")
        }
    "#};

    let expected = expect![[r#"
        model A {
          id Int @id
          b  B?  @relation("first")
          bs B?  @relation("second")
        }

        model B {
          id Int @id
          a1 A   @relation("first", fields: [id], references: [id], map: "first_fk")
          a2 A   @relation("second", fields: [id], references: [id], map: "second_fk")
        }
    "#]];

    expected.assert_eq(&api.re_introspect_dml(input_dm).await?);

    Ok(())
}
