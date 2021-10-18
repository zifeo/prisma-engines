use query_engine_tests::*;

#[test_suite(schema(schema))]
mod special_id_values {
    use indoc::indoc;
    use query_engine_tests::Runner;

    fn schema() -> String {
        let prisma = indoc! {r#"
        model Test {
            id Bytes @id @default(dbgenerated("(uuid_to_bin(uuid()))")) @test.Binary(16)
            name String
        }
        "#};

        prisma.to_string()
    }

    #[connector_test(only(MySql(8)))]
    async fn binary_uuid(runner: Runner) -> TestResult<()> {
        runner
            .query(indoc! {r#"
            mutation {
                createOneTest(data: {
                    name: "test"
                }) { id }
            }
            "#})
            .await?
            .assert_success();

        Ok(())
    }
}
