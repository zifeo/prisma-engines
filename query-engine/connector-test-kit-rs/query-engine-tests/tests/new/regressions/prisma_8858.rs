use query_engine_tests::*;

#[test_suite(schema(schema))]
mod update_connect {
    use indoc::indoc;
    use query_engine_tests::assert_query;

    fn schema() -> String {
        let schema = indoc! {r#"
        model Thing {
            id                 Int
            serie              String
            otherSerie         String?
            otherThing         Thing?  @relation(name: "thingToThing", fields: [id, otherSerie], references: [id, serie])
            otherThingReverse  Thing?  @relation(name: "thingToThing")
          
            @@id([id, serie])
          }
        "#};

        schema.to_owned()
    }

    #[connector_test]
    async fn test_connect_composite_key(runner: Runner) -> TestResult<()> {
        // Insert first thing
        runner
            .query(
                r#"
                mutation { 
                    createOneThing(
                        data: { id: 1 serie: "a" }
                    ) { id } 
                }"#,
            )
            .await?
            .assert_success();

        // Insert second thing relate to first thing
        runner
            .query(
                r#"
                mutation { 
                    createOneThing(
                        data: { id: 1 serie: "b" otherSerie: "a" }
                    ) { id } 
                }"#,
            )
            .await?
            .assert_success();

        
        // First thing should relate to the second thing, this will create a cyclic dependency
        // runner
        //     .query(
        //         r#"
        //             mutation { 
        //                 updateOneThing(
        //                     data: { otherSerie: "b" }, 
        //                     where: { 
        //                         id_serie: { id: 1 serie: "a" } 
        //                     }) { id }
        //             }"#,
        //     )
        //     .await?
        //     .assert_success();

        // ! This should trigger the error
        assert_query!(runner, r#"
        mutation { 
            updateOneThing(
                data: { 
                  otherThing: {
                    connect: { 
                      id_serie: { id: 1 serie: "b" } } } },
                where: {
                  id_serie: { id: 1 serie: "a" }
            }) { id, serie, otherSerie }
        }"#, r#"{"data":{"updateOneThing":{"id":1,"serie":"a","otherSerie":"b"}}}"#);

        Ok(())
    }
}
