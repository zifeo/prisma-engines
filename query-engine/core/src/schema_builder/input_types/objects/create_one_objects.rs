use super::fields::mapper::*;
use super::*;

#[tracing::instrument(skip(ctx, model, parent_field))]
pub(crate) fn create_one_input_types(
    ctx: &mut BuilderContext,
    model: &ModelRef,
    parent_field: Option<&RelationFieldRef>,
) -> Vec<InputType> {
    let checked_input = InputType::object(checked_create_input_type(ctx, model, parent_field));
    let unchecked_input = InputType::object(unchecked_create_input_type(ctx, model, parent_field));

    // If the inputs are equal, only use one.
    if checked_input == unchecked_input {
        vec![checked_input]
    } else {
        vec![checked_input, unchecked_input]
    }
}

/// Builds the create input type (<x>CreateInput / <x>CreateWithout<y>Input)
/// Also valid for nested inputs. A nested input is constructed if the `parent_field` is provided.
/// "Checked" input refers to disallowing writing relation scalars directly, as it can lead to unintended
/// data integrity violations if used incorrectly.
#[tracing::instrument(skip(ctx, model, parent_field))]
fn checked_create_input_type(
    ctx: &mut BuilderContext,
    model: &ModelRef,
    parent_field: Option<&RelationFieldRef>,
) -> InputObjectTypeWeakRef {
    // We allow creation from both sides of the relation - which would lead to an endless loop of input types
    // if we would allow to create the parent from a child create that is already a nested create.
    // To solve it, we remove the parent relation from the input ("Without<Parent>").
    let name = match parent_field.map(|pf| pf.related_field()) {
        Some(ref f) => format!("{}CreateWithout{}Input", model.name, capitalize(f.name.as_str())),
        _ => format!("{}CreateInput", model.name),
    };

    let ident = Identifier::new(name, PRISMA_NAMESPACE);
    return_cached_input!(ctx, &ident);

    let input_object = Arc::new(init_input_object_type(ident.clone()));
    ctx.cache_input_type(ident, input_object.clone());

    let filtered_fields = filter_checked_create_fields(model, parent_field);
    let field_mapper = CreateDataInputFieldMapper::new_checked();
    let input_fields = field_mapper.map_all(ctx, &filtered_fields);

    input_object.set_fields(input_fields);
    Arc::downgrade(&input_object)
}

/// Builds the create input type (<x>UncheckedCreateInput / <x>UncheckedCreateWithout<y>Input)
/// Also valid for nested inputs. A nested input is constructed if the `parent_field` is provided.
/// "Unchecked" input refers to allowing to write _all_ scalars on a model directly, which can
/// lead to unintended data integrity violations if used incorrectly.
#[tracing::instrument(skip(ctx, model, parent_field))]
fn unchecked_create_input_type(
    ctx: &mut BuilderContext,
    model: &ModelRef,
    parent_field: Option<&RelationFieldRef>,
) -> InputObjectTypeWeakRef {
    // We allow creation from both sides of the relation - which would lead to an endless loop of input types
    // if we would allow to create the parent from a child create that is already a nested create.
    // To solve it, we remove the parent relation from the input ("Without<Parent>").
    let name = match parent_field.map(|pf| pf.related_field()) {
        Some(ref f) => format!(
            "{}UncheckedCreateWithout{}Input",
            model.name,
            capitalize(f.name.as_str())
        ),
        _ => format!("{}UncheckedCreateInput", model.name),
    };

    let ident = Identifier::new(name, PRISMA_NAMESPACE);
    return_cached_input!(ctx, &ident);

    let input_object = Arc::new(init_input_object_type(ident.clone()));
    ctx.cache_input_type(ident, input_object.clone());

    let filtered_fields = filter_unchecked_create_fields(model, parent_field);
    let field_mapper = CreateDataInputFieldMapper::new_unchecked();
    let input_fields = field_mapper.map_all(ctx, &filtered_fields);

    input_object.set_fields(input_fields);
    Arc::downgrade(&input_object)
}

/// Filters the given model's fields down to the allowed ones for checked create.
fn filter_checked_create_fields(model: &ModelRef, parent_field: Option<&RelationFieldRef>) -> Vec<ModelField> {
    model
        .fields()
        .all
        .iter()
        .filter(|field| match field {
            // Scalars must be writable and not an autogenerated ID, which are disallowed for checked inputs
            // regardless of whether or not the connector supports it.
            ModelField::Scalar(sf) => !sf.is_auto_generated_int_id && !sf.is_read_only(),

            // If the relation field `rf` is the one that was traversed to by the parent relation field `parent_field`,
            // then exclude it for checked inputs - this prevents endless nested type circles that are useless to offer as API.
            ModelField::Relation(rf) => {
                let field_was_traversed_to = parent_field.filter(|pf| pf.related_field().name == rf.name).is_some();
                !field_was_traversed_to
            }

            // Always keep composites
            ModelField::Composite(_) => true,
        })
        .map(Clone::clone)
        .collect()
}

/// Filters the given model's fields down to the allowed ones for unchecked create.
fn filter_unchecked_create_fields(model: &ModelRef, parent_field: Option<&RelationFieldRef>) -> Vec<ModelField> {
    let linking_fields = if let Some(parent_field) = parent_field {
        let child_field = parent_field.related_field();
        if child_field.is_inlined_on_enclosing_model() {
            child_field
                .linking_fields()
                .as_scalar_fields()
                .expect("Expected linking fields to be scalar.")
        } else {
            vec![]
        }
    } else {
        vec![]
    };

    model
        .fields()
        .all
        .iter()
        .filter(|field| match field {
            // In principle, all scalars are writable for unchecked inputs. However, it still doesn't make any sense to be able to write the scalars that
            // link the model to the parent record in case of a nested unchecked create, as this would introduce complexities we don't want to deal with right now.
            ModelField::Scalar(sf) => !linking_fields.contains(sf),

            // If the relation field `rf` is the one that was traversed to by the parent relation field `parent_field`,
            // then exclude it for checked inputs - this prevents endless nested type circles that are useless to offer as API.
            //
            // Additionally, only relations that point to other models and are NOT inlined on the currently in scope model are allowed in the unchecked input, because if they are
            // inlined, they are written only as scalars for unchecked, not via the relation API (`connect`, nested `create`, etc.).
            ModelField::Relation(rf) => {
                let is_not_inlined = !rf.is_inlined_on_enclosing_model();
                let field_was_not_traversed_to = parent_field.filter(|pf| pf.related_field().name == rf.name).is_none();

                field_was_not_traversed_to && is_not_inlined
            }

            // Always keep composites
            ModelField::Composite(_) => true,
        })
        .map(Clone::clone)
        .collect()
}
