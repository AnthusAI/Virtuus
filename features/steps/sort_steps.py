from behave import given, then, when
from virtuus._python.sort import Sort


def _coerce(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        pass
    try:
        return float(value)
    except (ValueError, TypeError):
        pass
    return value


@given(u'a sort condition of "{op}" with value "{value}"')
def step_sort_condition_single(context, op, value):
    factory = getattr(Sort, op)
    context.predicate = factory(_coerce(value))


@given(u'a sort condition of "begins_with" with value ""')
def step_sort_begins_with_empty(context):
    context.predicate = Sort.begins_with("")


@given(u'a sort condition of "contains" with value ""')
def step_sort_contains_empty(context):
    context.predicate = Sort.contains("")


@given(u'a sort condition of "eq" with value ""')
def step_sort_eq_empty(context):
    context.predicate = Sort.eq("")


@given('a sort condition of "between" with low "{low}" and high "{high}"')
def step_sort_condition_between(context, low, high):
    context.predicate = Sort.between(_coerce(low), _coerce(high))


@when(u'evaluated against "{input_value}"')
def step_evaluate_against_string(context, input_value):
    context.result = context.predicate(_coerce(input_value))


@when(u'evaluated against ""')
def step_evaluate_against_empty(context):
    context.result = context.predicate("")


@when("evaluated against a null value")
def step_evaluate_against_null(context):
    context.result = context.predicate(None)


@then("the result should be true")
def step_result_true(context):
    assert context.result is True, f"Expected True but got {context.result!r}"


@then("the result should be false")
def step_result_false(context):
    assert context.result is False, f"Expected False but got {context.result!r}"
