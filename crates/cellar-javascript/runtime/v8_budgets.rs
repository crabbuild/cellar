//! Private V8 callback data binds each control to one native invocation/budget.
use super::*;

fn ids(scope: &mut v8::PinScope, args: &v8::FunctionCallbackArguments) -> Option<(u64, u64)> {
    let data = v8::Local::<v8::Array>::try_from(args.data()).ok()?;
    let invocation = data.get_index(scope, 0)?.number_value(scope)? as u64;
    let id = data.get_index(scope, 1)?.number_value(scope)? as u64;
    Some((invocation, id))
}

fn authority(scope: &mut v8::PinScope, invocation: u64) -> Result<Shared, &'static str> {
    if scope
        .get_continuation_preserved_embedder_data()
        .number_value(scope)
        != Some(invocation as f64)
    {
        return Err("ContextMismatch");
    }
    let state = scope
        .get_slot::<Shared>()
        .cloned()
        .ok_or("ContextExpired")?;
    let valid = {
        let state = state.borrow();
        !state.runtime.closed.load(Ordering::Acquire)
            && state.invocations.get(&invocation).is_some_and(|active| {
                active.start.active.load(Ordering::Acquire)
                    && active.start.owner.load(Ordering::Acquire)
            })
    };
    if valid {
        Ok(state)
    } else {
        Err("ContextExpired")
    }
}

fn remaining(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue,
) {
    let Some((invocation, id)) = ids(scope, &args) else {
        return refuse(scope, "ContextMismatch");
    };
    let result = authority(scope, invocation)
        .and_then(|state| state.borrow().budgets.remaining(invocation, id));
    match result {
        Ok(value) => rv.set(v8::Number::new(scope, f64::from(value)).into()),
        Err(error) => refuse(scope, error),
    }
}

fn limit(scope: &mut v8::PinScope, args: v8::FunctionCallbackArguments, _rv: v8::ReturnValue) {
    let Some((invocation, id)) = ids(scope, &args) else {
        return refuse(scope, "ContextMismatch");
    };
    if !args.get(0).is_uint32() {
        return refuse(scope, "InvalidArgument");
    }
    let Some(timeout) = args.get(0).uint32_value(scope) else {
        return refuse(scope, "InvalidArgument");
    };
    let result = authority(scope, invocation)
        .and_then(|state| state.borrow_mut().budgets.limit(invocation, id, timeout));
    if let Err(error) = result {
        refuse(scope, error);
    }
}

fn release(scope: &mut v8::PinScope, args: v8::FunctionCallbackArguments, _rv: v8::ReturnValue) {
    // Cleanup may run after revocation or from another continuation. It can
    // remove only this capability and never submit an SDK effect.
    if let Some((invocation, id)) = ids(scope, &args) {
        if let Some(state) = scope.get_slot::<Shared>() {
            state.borrow_mut().budgets.release(invocation, id);
        }
    }
}

pub(super) fn begin(scope: &mut v8::PinScope, invocation: u64, mut rv: v8::ReturnValue) {
    let state = match authority(scope, invocation) {
        Ok(state) => state,
        Err(error) => return refuse(scope, error),
    };
    let (runtime, source) = {
        let state = state.borrow();
        (
            state.runtime.clone(),
            state.invocations[&invocation].start.budget_source.clone(),
        )
    };
    let Ok(id) = runtime.id() else {
        return refuse(scope, "OperationLimit");
    };
    let owner = v8::Number::new(scope, invocation as f64);
    let key = v8::Number::new(scope, id as f64);
    let data = v8::Array::new_with_elements(scope, &[owner.into(), key.into()]);
    let Some(read) = v8::Function::builder(remaining)
        .data(data.into())
        .build(scope)
    else {
        return refuse(scope, "RuntimeFailed");
    };
    let Some(cap) = v8::Function::builder(limit).data(data.into()).build(scope) else {
        return refuse(scope, "RuntimeFailed");
    };
    let Some(drop) = v8::Function::builder(release)
        .data(data.into())
        .build(scope)
    else {
        return refuse(scope, "RuntimeFailed");
    };
    let started = state.borrow_mut().budgets.start(
        invocation,
        id,
        source.as_ref(),
        runtime.options.max_pending_operations,
    );
    if let Err(error) = started {
        return refuse(scope, error);
    }
    rv.set(
        v8::Array::new_with_elements(scope, &[key.into(), read.into(), cap.into(), drop.into()])
            .into(),
    );
}
