use crate::{
    ast, dml,
    transform::ast_to_dml::db::{self, walkers::*, IndexAlgorithm},
    IndexField, PrimaryKeyField,
};
use ::dml::composite_type::{CompositeType, CompositeTypeField, CompositeTypeFieldType};
use datamodel_connector::{walker_ext_traits::*, Connector, ReferentialIntegrity};
use std::collections::HashMap;

/// Helper for lifting a datamodel.
///
/// When lifting, the AST is converted to the Datamodel data structure, and
/// additional semantics are attached.
pub(crate) struct LiftAstToDml<'a> {
    db: &'a db::ParserDatabase<'a>,
    connector: &'static dyn Connector,
    referential_integrity: ReferentialIntegrity,
}

impl<'a> LiftAstToDml<'a> {
    pub(crate) fn new(
        db: &'a db::ParserDatabase<'a>,
        connector: &'static dyn Connector,
        referential_integrity: ReferentialIntegrity,
    ) -> LiftAstToDml<'a> {
        LiftAstToDml {
            db,
            connector,
            referential_integrity,
        }
    }

    pub(crate) fn lift(&self) -> dml::Datamodel {
        let mut schema = dml::Datamodel::new();

        // We iterate over scalar fields, then relations, but we want the
        // order of fields in the dml::Model to match the order of the fields in
        // the AST, so we need this bit of extra bookkeeping.
        //
        // (model_idx, field_name) -> sort_key
        let mut field_ids_for_sorting: HashMap<(&str, &str), ast::FieldId> = HashMap::new();

        for model in self.db.walk_models() {
            schema.add_model(self.lift_model(model, &mut field_ids_for_sorting));
        }

        for composite_type in self.db.walk_composite_types() {
            schema.composite_types.push(self.lift_composite_type(composite_type))
        }

        for r#enum in self.db.walk_enums() {
            schema.add_enum(self.lift_enum(r#enum))
        }

        self.lift_relations(&mut schema, &mut field_ids_for_sorting);

        for model in schema.models_mut() {
            let model_name = model.name.as_str();
            model
                .fields
                .sort_by_key(|field| field_ids_for_sorting.get(&(model_name, field.name())).cloned());
        }

        schema
    }

    fn lift_relations(
        &self,
        schema: &mut dml::Datamodel,
        field_ids_for_sorting: &mut HashMap<(&'a str, &'a str), ast::FieldId>,
    ) {
        let active_connector = self.connector;
        let referential_integrity = self.referential_integrity;
        let common_dml_fields = |field: &mut dml::RelationField, relation_field: RelationFieldWalker<'_, '_>| {
            let ast_field = relation_field.ast_field();
            field.relation_info.on_delete = relation_field
                .explicit_on_delete()
                .map(parser_database_referential_action_to_dml_referential_action);
            field.relation_info.on_update = relation_field
                .explicit_on_update()
                .map(parser_database_referential_action_to_dml_referential_action);
            field.relation_info.name = relation_field.relation_name().to_string();
            field.documentation = ast_field.documentation.clone().map(|comment| comment.text);
            field.is_ignored = relation_field.is_ignored();
            field.supports_restrict_action(
                active_connector
                    .supports_referential_action(&referential_integrity, parser_database::ReferentialAction::Restrict),
            );
            field.emulates_referential_actions(referential_integrity.is_prisma());
        };

        for relation in self.db.walk_relations() {
            match relation.refine() {
                RefinedRelationWalker::Inline(relation) => {
                    // Forward field
                    {
                        let relation_info = dml::RelationInfo::new(relation.referenced_model().name());
                        let model = schema.find_model_mut(relation.referencing_model().name());
                        let mut inferred_scalar_fields = Vec::new(); // reformatted/virtual/inferred extra scalar fields for reformatted relations.

                        let mut relation_field = if let Some(relation_field) = relation.forward_relation_field() {
                            // Construct a relation field in the DML for an existing relation field in the source.
                            let arity = self.lift_field_arity(&relation_field.ast_field().arity);
                            let referential_arity = self.lift_field_arity(&relation_field.referential_arity());
                            let mut field =
                                dml::RelationField::new(relation_field.name(), arity, referential_arity, relation_info);

                            field.relation_info.fk_name = Some(relation.constraint_name(active_connector).into_owned());
                            common_dml_fields(&mut field, relation_field);
                            field_ids_for_sorting.insert(
                                (relation_field.model().name(), relation_field.name()),
                                relation_field.field_id(),
                            );

                            field
                        } else {
                            // Construct a relation field in the DML without corresponding relation field in the source.
                            //
                            // This is part of magic reformatting.
                            let arity = self.lift_field_arity(&relation.forward_relation_field_arity());
                            let referential_arity = arity;
                            dml::RelationField::new(
                                relation.referenced_model().name(),
                                arity,
                                referential_arity,
                                relation_info,
                            )
                        };

                        relation_field.relation_info.name = relation.relation_name().to_string();

                        relation_field.relation_info.references = relation
                            .referenced_fields()
                            .map(|field| field.name().to_owned())
                            .collect();

                        relation_field.relation_info.fields = match relation.referencing_fields() {
                            ReferencingFields::Concrete(fields) => fields.map(|f| f.name().to_owned()).collect(),
                            ReferencingFields::Inferred(fields) => {
                                // In this branch, we are creating the underlying scalar fields
                                // from thin air.  This is part of reformatting.
                                let mut field_names = Vec::with_capacity(fields.len());

                                for field in fields {
                                    let field_type = self.lift_scalar_field_type(
                                        field.blueprint.ast_field(),
                                        &field.tpe,
                                        field.blueprint,
                                    );
                                    let mut scalar_field = dml::ScalarField::new_generated(&field.name, field_type);
                                    scalar_field.arity = if relation_field.arity.is_required() {
                                        dml::FieldArity::Required
                                    } else {
                                        self.lift_field_arity(&field.arity)
                                    };
                                    inferred_scalar_fields.push(dml::Field::ScalarField(scalar_field));

                                    field_names.push(field.name);
                                }

                                field_names
                            }
                            ReferencingFields::NA => Vec::new(),
                        };
                        model.add_field(dml::Field::RelationField(relation_field));

                        for field in inferred_scalar_fields {
                            model.add_field(field)
                        }
                    };

                    // Back field
                    {
                        let relation_info = dml::RelationInfo::new(relation.referencing_model().name());
                        let model = schema.find_model_mut(relation.referenced_model().name());

                        let mut field = if let Some(relation_field) = relation.back_relation_field() {
                            let ast_field = relation_field.ast_field();
                            let arity = self.lift_field_arity(&ast_field.arity);
                            let referential_arity = self.lift_field_arity(&relation_field.referential_arity());
                            let mut field =
                                dml::RelationField::new(relation_field.name(), arity, referential_arity, relation_info);

                            common_dml_fields(&mut field, relation_field);

                            field_ids_for_sorting.insert(
                                (relation_field.model().name(), relation_field.name()),
                                relation_field.field_id(),
                            );

                            field
                        } else {
                            // This is part of reformatting.
                            let arity = dml::FieldArity::List;
                            let referential_arity = dml::FieldArity::List;
                            let mut field = dml::RelationField::new(
                                relation.referencing_model().name(),
                                arity,
                                referential_arity,
                                relation_info,
                            );
                            field.is_ignored = relation.referencing_model().is_ignored();
                            field
                        };

                        field.relation_info.name = relation.relation_name().to_string();
                        model.add_field(dml::Field::RelationField(field));
                    };
                }
                RefinedRelationWalker::ImplicitManyToMany(relation) => {
                    for relation_field in [relation.field_a(), relation.field_b()] {
                        let ast_field = relation_field.ast_field();
                        let arity = self.lift_field_arity(&ast_field.arity);
                        let relation_info = dml::RelationInfo::new(relation_field.related_model().name());
                        let referential_arity = self.lift_field_arity(&relation_field.referential_arity());
                        let mut field =
                            dml::RelationField::new(relation_field.name(), arity, referential_arity, relation_info);

                        common_dml_fields(&mut field, relation_field);

                        let primary_key = relation_field.related_model().primary_key().unwrap();
                        field.relation_info.references =
                            primary_key.fields().map(|field| field.name().to_owned()).collect();

                        field.relation_info.fields = relation_field
                            .fields()
                            .into_iter()
                            .flatten()
                            .map(|f| f.name().to_owned())
                            .collect();

                        let model = schema.find_model_mut(relation_field.model().name());
                        model.add_field(dml::Field::RelationField(field));
                        field_ids_for_sorting.insert(
                            (relation_field.model().name(), relation_field.name()),
                            relation_field.field_id(),
                        );
                    }
                }
            }
        }
    }

    fn lift_composite_type(&self, walker: CompositeTypeWalker<'_, '_>) -> CompositeType {
        let mut fields = Vec::new();

        for field in walker.fields() {
            let field = CompositeTypeField {
                name: field.name().to_owned(),
                r#type: self.lift_composite_type_field_type(field.r#type()),
                arity: self.lift_field_arity(&field.arity()),
                database_name: field.mapped_name().map(String::from),
                documentation: field.documentation().map(ToString::to_string),
            };

            fields.push(field);
        }

        CompositeType {
            name: walker.name().to_owned(),
            fields,
        }
    }

    /// Internal: Validates a model AST node and lifts it to a DML model.
    fn lift_model(
        &self,
        walker: ModelWalker<'a, '_>,
        field_ids_for_sorting: &mut HashMap<(&'a str, &'a str), ast::FieldId>,
    ) -> dml::Model {
        let ast_model = walker.ast_model();
        let mut model = dml::Model::new(ast_model.name.name.clone(), None);

        model.documentation = ast_model.documentation.clone().map(|comment| comment.text);
        model.database_name = walker.mapped_name().map(String::from);
        model.is_ignored = walker.is_ignored();

        model.primary_key = walker.primary_key().map(|pk| dml::PrimaryKeyDefinition {
            name: pk.name().map(String::from),
            db_name: pk.final_database_name(self.connector).map(|c| c.into_owned()),
            fields: pk
                .scalar_field_attributes()
                .map(|field|
                    //TODO (extended indexes) here it is ok to pass sort and length with out a preview flag
                    // check since this is coming from the ast and the parsing would reject the args without
                    // the flag set
                    // When we start using the extra args here could be a place to fill in the defaults. 
                    PrimaryKeyField {
                    name: field.as_scalar_field().name().to_owned(),
                    sort_order: field.sort_order().map(parser_database_sort_order_to_dml_sort_order),
                    length: field.length(),
                })
                .collect(),
            defined_on_field: pk.is_defined_on_field(),
        });

        model.indices = walker
            .indexes()
            .map(|idx| {
                assert!(idx.fields().len() != 0);

                let fields = idx
                    .scalar_field_attributes()
                    .map(|field| IndexField {
                        name: field.as_scalar_field().name().to_owned(),
                        sort_order: field.sort_order().map(parser_database_sort_order_to_dml_sort_order),
                        length: field.length(),
                    })
                    .collect();

                let tpe = match idx.index_type() {
                    db::IndexType::Unique => dml::IndexType::Unique,
                    db::IndexType::Normal => dml::IndexType::Normal,
                    db::IndexType::Fulltext => dml::IndexType::Fulltext,
                };

                let algorithm = idx.algorithm().map(|using| match using {
                    IndexAlgorithm::BTree => dml::IndexAlgorithm::BTree,
                    IndexAlgorithm::Hash => dml::IndexAlgorithm::Hash,
                });

                dml::IndexDefinition {
                    name: idx.name().map(String::from),
                    db_name: Some(idx.final_database_name(self.connector).into_owned()),
                    fields,
                    tpe,
                    algorithm,
                    defined_on_field: idx.is_defined_on_field(),
                }
            })
            .collect();

        for scalar_field in walker.scalar_fields() {
            let field_id = scalar_field.field_id();
            let ast_field = &ast_model[field_id];
            let arity = self.lift_field_arity(&ast_field.arity);
            let field_type = match &scalar_field.scalar_field_type() {
                db::ScalarFieldType::CompositeType(ctid) => {
                    let mut field = dml::CompositeField::new();
                    field.composite_type = self.db.ast()[*ctid].name.name.to_owned();
                    field.documentation = ast_field.documentation.clone().map(|comment| comment.text);
                    field.is_ignored = scalar_field.is_ignored();
                    field.database_name = scalar_field.mapped_name().map(String::from);

                    model.add_field(dml::Field::CompositeField(field));
                    continue;
                }
                _ => self.lift_scalar_field_type(ast_field, &scalar_field.scalar_field_type(), scalar_field),
            };

            let mut field = dml::ScalarField::new(&ast_field.name.name, arity, field_type);

            field.documentation = ast_field.documentation.clone().map(|comment| comment.text);
            field.is_ignored = scalar_field.is_ignored();
            field.is_updated_at = scalar_field.is_updated_at();
            field.database_name = scalar_field.mapped_name().map(String::from);
            field.default_value = scalar_field.default_value().map(|d| dml::DefaultValue {
                kind: d.dml_default_kind(),
                db_name: Some(d.constraint_name(self.connector).into())
                    .filter(|_| self.connector.supports_named_default_values()),
            });

            field_ids_for_sorting.insert((&ast_model.name.name, &ast_field.name.name), field_id);
            model.add_field(dml::Field::ScalarField(field));
        }

        model
    }

    /// Internal: Validates an enum AST node.
    fn lift_enum(&self, r#enum: EnumWalker<'_, '_>) -> dml::Enum {
        let mut en = dml::Enum::new(r#enum.name(), vec![]);

        for value in r#enum.values() {
            en.add_value(self.lift_enum_value(value));
        }

        en.documentation = r#enum.ast_enum().documentation.clone().map(|comment| comment.text);
        en.database_name = r#enum.mapped_name().map(String::from);
        en
    }

    /// Internal: Lifts an enum value AST node.
    fn lift_enum_value(&self, value: EnumValueWalker<'_, '_>) -> dml::EnumValue {
        let mut enum_value = dml::EnumValue::new(value.name());
        enum_value.documentation = value.documentation().map(String::from);
        enum_value.database_name = value.mapped_name().map(String::from);
        enum_value
    }

    /// Internal: Lift a field's arity.
    fn lift_field_arity(&self, field_arity: &ast::FieldArity) -> dml::FieldArity {
        match field_arity {
            ast::FieldArity::Required => dml::FieldArity::Required,
            ast::FieldArity::Optional => dml::FieldArity::Optional,
            ast::FieldArity::List => dml::FieldArity::List,
        }
    }

    fn lift_scalar_field_type(
        &self,
        ast_field: &ast::Field,
        scalar_field_type: &db::ScalarFieldType,
        scalar_field: ScalarFieldWalker<'_, '_>,
    ) -> dml::FieldType {
        match scalar_field_type {
            db::ScalarFieldType::CompositeType(_) => {
                unreachable!();
            }
            db::ScalarFieldType::Enum(enum_id) => {
                let enum_name = &self.db.ast()[*enum_id].name.name;
                dml::FieldType::Enum(enum_name.to_owned())
            }
            db::ScalarFieldType::Unsupported => {
                dml::FieldType::Unsupported(ast_field.field_type.as_unsupported().unwrap().0.to_owned())
            }
            db::ScalarFieldType::Alias(top_id) => {
                let alias = &self.db.ast()[*top_id];
                let scalar_field_type = self.db.alias_scalar_field_type(top_id);
                self.lift_scalar_field_type(alias, scalar_field_type, scalar_field)
            }
            db::ScalarFieldType::BuiltInScalar(scalar_type) => {
                let native_type = scalar_field
                    .raw_native_type()
                    .map(|(_, name, args, _)| self.connector.parse_native_type(name, args.to_owned()).unwrap());
                dml::FieldType::Scalar(
                    parser_database_scalar_type_to_dml_scalar_type(*scalar_type),
                    None,
                    native_type,
                )
            }
        }
    }

    fn lift_composite_type_field_type(&self, scalar_field_type: &db::ScalarFieldType) -> CompositeTypeFieldType {
        match scalar_field_type {
            db::ScalarFieldType::CompositeType(ctid) => {
                CompositeTypeFieldType::CompositeType(self.db.ast()[*ctid].name.name.to_owned())
            }
            db::ScalarFieldType::BuiltInScalar(scalar_type) => {
                CompositeTypeFieldType::Scalar(parser_database_scalar_type_to_dml_scalar_type(*scalar_type), None, None)
            }
            db::ScalarFieldType::Alias(_) | db::ScalarFieldType::Enum(_) | db::ScalarFieldType::Unsupported => {
                unreachable!()
            }
        }
    }
}

fn parser_database_sort_order_to_dml_sort_order(sort_order: parser_database::SortOrder) -> dml::SortOrder {
    match sort_order {
        parser_database::SortOrder::Asc => dml::SortOrder::Asc,
        parser_database::SortOrder::Desc => dml::SortOrder::Desc,
    }
}

fn parser_database_referential_action_to_dml_referential_action(
    ra: parser_database::ReferentialAction,
) -> dml::ReferentialAction {
    match ra {
        parser_database::ReferentialAction::Cascade => dml::ReferentialAction::Cascade,
        parser_database::ReferentialAction::SetNull => dml::ReferentialAction::SetNull,
        parser_database::ReferentialAction::SetDefault => dml::ReferentialAction::SetDefault,
        parser_database::ReferentialAction::Restrict => dml::ReferentialAction::Restrict,
        parser_database::ReferentialAction::NoAction => dml::ReferentialAction::NoAction,
    }
}

fn parser_database_scalar_type_to_dml_scalar_type(st: parser_database::ScalarType) -> dml::ScalarType {
    st.as_str().parse().unwrap()
}
