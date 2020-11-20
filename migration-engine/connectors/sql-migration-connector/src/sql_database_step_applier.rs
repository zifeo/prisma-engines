use crate::{
    pair::Pair,
    sql_migration::{CreateTable, DropTable, SqlMigration, SqlMigrationStep},
    SqlFlavour, SqlMigrationConnector,
};
use migration_connector::{
    ConnectorResult, DatabaseMigrationMarker, DatabaseMigrationStepApplier, DestructiveChangeDiagnostics,
    PrettyDatabaseMigrationStep,
};
use once_cell::sync::Lazy;
use regex::Regex;
use sql_schema_describer::{walkers::SqlSchemaExt, SqlSchema};
use std::ops::Index;

#[async_trait::async_trait]
impl DatabaseMigrationStepApplier<SqlMigration> for SqlMigrationConnector {
    #[tracing::instrument(skip(self, database_migration))]
    async fn apply_step(&self, database_migration: &SqlMigration, index: usize) -> ConnectorResult<bool> {
        self.apply_next_step(
            &database_migration.steps,
            index,
            self.flavour(),
            database_migration.schemas(),
        )
        .await
    }

    fn render_steps_pretty(
        &self,
        database_migration: &SqlMigration,
    ) -> ConnectorResult<Vec<PrettyDatabaseMigrationStep>> {
        render_steps_pretty(&database_migration, self.flavour(), database_migration.schemas())
    }

    fn render_script(&self, database_migration: &SqlMigration, diagnostics: &DestructiveChangeDiagnostics) -> String {
        const NEWLINE: char = '\n';

        if database_migration.is_empty() {
            return "-- This is an empty migration.".to_string();
        }

        let mut script = String::with_capacity(40 * database_migration.steps.len());

        // Note: it would be much nicer if we could place the warnings next to
        // the SQL for the steps that triggered them.
        if diagnostics.has_warnings() || !diagnostics.unexecutable_migrations.is_empty() {
            script.push_str("/*\n  Warnings:\n\n");

            for warning in &diagnostics.warnings {
                script.push_str("  - ");
                script.push_str(&warning.description);
                script.push(NEWLINE);
            }

            for unexecutable in &diagnostics.unexecutable_migrations {
                script.push_str("  - ");
                script.push_str(&unexecutable.description);
                script.push(NEWLINE);
            }

            script.push_str("\n*/\n")
        }

        for step in &database_migration.steps {
            let statements: Vec<String> = render_raw_sql(
                step,
                self.flavour(),
                Pair::new(&database_migration.before, &database_migration.after),
            );

            if !statements.is_empty() {
                script.push_str(format!("-- [Step: {}]", step.description()).as_str());
                script.push(NEWLINE);

                for statement in statements {
                    script.push_str(&statement);
                    script.push_str(";\n");
                }
            }
        }

        script
    }

    async fn apply_script(&self, script: &str) -> ConnectorResult<()> {
        static STEP_SEPARATOR_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r#"^-- \[Step: (.*)\]$"#).unwrap());

        let lines = script.lines().enumerate();

        let statements = lines.fold(vec![(0, "".to_string(), "".to_string())], |mut acc, (index, line)| {
            match STEP_SEPARATOR_RE.captures(line) {
                Some(t) if t.get(1).is_some() => acc.push((index, t.index(1).to_string(), "".to_string())),
                _ => {
                    acc.last_mut().unwrap().2.push_str(line);
                    acc.last_mut().unwrap().2.push('\n')
                }
            };
            acc
        });

        for (index, description, statement) in statements.iter().skip(1) {
            println!("{} {} \n {}", index, description, statement);

            //catch error, unite with line information from statement
            self.conn().raw_cmd(statement).await?
        }

        Ok(())
    }
}

impl SqlMigrationConnector {
    async fn apply_next_step(
        &self,
        steps: &[SqlMigrationStep],
        index: usize,
        renderer: &(dyn SqlFlavour + Send + Sync),
        schemas: Pair<&SqlSchema>,
    ) -> ConnectorResult<bool> {
        let has_this_one = steps.get(index).is_some();

        if !has_this_one {
            return Ok(false);
        }

        let step = &steps[index];
        tracing::debug!(?step);

        for sql_string in render_raw_sql(&step, renderer, schemas) {
            tracing::debug!(index, %sql_string);

            self.conn().raw_cmd(&sql_string).await?;
        }

        Ok(true)
    }
}

fn render_steps_pretty(
    database_migration: &SqlMigration,
    renderer: &(dyn SqlFlavour + Send + Sync),
    schemas: Pair<&SqlSchema>,
) -> ConnectorResult<Vec<PrettyDatabaseMigrationStep>> {
    let mut steps = Vec::with_capacity(database_migration.steps.len());

    for step in &database_migration.steps {
        let sql = render_raw_sql(&step, renderer, schemas).join(";\n");

        if !sql.is_empty() {
            steps.push(PrettyDatabaseMigrationStep {
                step: serde_json::to_value(&step).unwrap_or_else(|_| serde_json::json!({})),
                raw: sql,
            });
        }
    }

    Ok(steps)
}

fn render_raw_sql(
    step: &SqlMigrationStep,
    renderer: &(dyn SqlFlavour + Send + Sync),
    schemas: Pair<&SqlSchema>,
) -> Vec<String> {
    match step {
        SqlMigrationStep::AlterEnum(alter_enum) => renderer.render_alter_enum(alter_enum, &schemas),
        SqlMigrationStep::RedefineTables(redefine_tables) => renderer.render_redefine_tables(redefine_tables, &schemas),
        SqlMigrationStep::CreateEnum(create_enum) => {
            renderer.render_create_enum(&schemas.next().enum_walker_at(create_enum.enum_index))
        }
        SqlMigrationStep::DropEnum(drop_enum) => {
            renderer.render_drop_enum(&schemas.previous().enum_walker_at(drop_enum.enum_index))
        }
        SqlMigrationStep::CreateTable(CreateTable { table_index }) => {
            let table = schemas.next().table_walker_at(*table_index);

            vec![renderer.render_create_table(&table)]
        }
        SqlMigrationStep::DropTable(DropTable { table_index }) => {
            renderer.render_drop_table(schemas.previous().table_walker_at(*table_index).name())
        }
        SqlMigrationStep::RedefineIndex { table, index } => {
            renderer.render_drop_and_recreate_index(schemas.tables(table).indexes(index).as_ref())
        }
        SqlMigrationStep::AddForeignKey(add_foreign_key) => {
            let foreign_key = schemas
                .next()
                .table_walker_at(add_foreign_key.table_index)
                .foreign_key_at(add_foreign_key.foreign_key_index);
            vec![renderer.render_add_foreign_key(&foreign_key)]
        }
        SqlMigrationStep::DropForeignKey(drop_foreign_key) => vec![renderer.render_drop_foreign_key(drop_foreign_key)],
        SqlMigrationStep::AlterTable(alter_table) => renderer.render_alter_table(alter_table, &schemas),
        SqlMigrationStep::CreateIndex(create_index) => vec![renderer.render_create_index(
            &schemas
                .next()
                .table_walker_at(create_index.table_index)
                .index_at(create_index.index_index),
        )],
        SqlMigrationStep::DropIndex(drop_index) => vec![renderer.render_drop_index(drop_index)],
        SqlMigrationStep::AlterIndex { table, index } => {
            renderer.render_alter_index(schemas.tables(table).indexes(index).as_ref())
        }
    }
}
