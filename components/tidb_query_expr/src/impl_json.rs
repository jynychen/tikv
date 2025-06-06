// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::BTreeMap;

use serde::de::IgnoredAny;
use tidb_query_codegen::rpn_fn;
use tidb_query_common::Result;
use tidb_query_datatype::{
    EvalType,
    codec::{data_type::*, mysql::json::*},
};

#[rpn_fn]
#[inline]
fn json_depth(arg: JsonRef) -> Result<Option<i64>> {
    Ok(Some(arg.depth()?))
}

#[rpn_fn(writer)]
#[inline]
fn json_type(arg: JsonRef, writer: BytesWriter) -> Result<BytesGuard> {
    Ok(writer.write(Some(Bytes::from(arg.json_type()))))
}

#[rpn_fn(nullable, raw_varg, min_args = 2, extra_validator = json_modify_validator)]
#[inline]
fn json_set(args: &[ScalarValueRef]) -> Result<Option<Json>> {
    json_modify(args, ModifyType::Set)
}

#[rpn_fn(nullable, raw_varg, min_args = 2, extra_validator = json_modify_validator)]
#[inline]
fn json_insert(args: &[ScalarValueRef]) -> Result<Option<Json>> {
    json_modify(args, ModifyType::Insert)
}

#[rpn_fn(nullable, raw_varg, min_args = 2, extra_validator = json_modify_validator)]
#[inline]
fn json_replace(args: &[ScalarValueRef]) -> Result<Option<Json>> {
    json_modify(args, ModifyType::Replace)
}

#[inline]
fn json_modify(args: &[ScalarValueRef], mt: ModifyType) -> Result<Option<Json>> {
    assert!(args.len() >= 2);
    // base Json argument
    let base: Option<JsonRef> = args[0].as_json();
    let base = base.map_or(Json::none(), |json| Ok(json.to_owned()))?;

    let buf_size = args.len() / 2;

    let mut path_expr_list = Vec::with_capacity(buf_size);
    let mut values = Vec::with_capacity(buf_size);

    for chunk in args[1..].chunks(2) {
        let path: Option<BytesRef> = chunk[0].as_bytes();
        let value: Option<JsonRef> = chunk[1].as_json();

        path_expr_list.push(try_opt!(parse_json_path(path)));

        let value = value
            .as_ref()
            .map_or(Json::none(), |json| Ok(json.to_owned()))?;
        values.push(value);
    }
    Ok(Some(base.as_ref().modify(&path_expr_list, values, mt)?))
}

#[rpn_fn(nullable, raw_varg, min_args = 2, extra_validator = json_modify_validator)]
#[inline]
fn json_array_append(args: &[ScalarValueRef]) -> Result<Option<Json>> {
    assert!(args.len() >= 2);
    // Returns None if Base is None
    if args[0].to_owned().is_none() {
        return Ok(None);
    }
    // base Json argument
    let base: Option<JsonRef> = args[0].as_json();
    let mut base = base.map_or(Json::none(), |json| Ok(json.to_owned()))?;

    for chunk in args[1..].chunks(2) {
        let path: Option<BytesRef> = chunk[0].as_bytes();
        let value: Option<JsonRef> = chunk[1].as_json();

        let value = value
            .as_ref()
            .map_or(Json::none(), |json| Ok(json.to_owned()))?;
        // extract the element from the path, then merge the value into the element
        // 1. extrace the element from the path
        let tmp_path_expr_list = vec![try_opt!(parse_json_path(path))];
        let element: Option<Json> = base.as_ref().extract(&tmp_path_expr_list)?;
        // 2. merge the value into the element
        if let Some(elem) = element {
            // if both elem and value are json object, wrap elem into a vector
            if elem.get_type() == JsonType::Object && value.get_type() == JsonType::Object {
                let array_json: Json = Json::from_array(vec![elem.clone()])?;
                let tmp_values = vec![array_json.as_ref(), value.as_ref()];
                let tmp_value = Json::merge(tmp_values)?;
                base =
                    base.as_ref()
                        .modify(&tmp_path_expr_list, vec![tmp_value], ModifyType::Set)?;
            } else {
                let tmp_values = vec![elem.as_ref(), value.as_ref()];
                let tmp_value = Json::merge(tmp_values)?;
                base =
                    base.as_ref()
                        .modify(&tmp_path_expr_list, vec![tmp_value], ModifyType::Set)?;
            }
        }
    }
    Ok(Some(base))
}

/// validate the arguments are `(Option<JsonRef>, &[(Option<Bytes>,
/// Option<Json>)])`
fn json_modify_validator(expr: &tipb::Expr) -> Result<()> {
    let children = expr.get_children();
    assert!(children.len() >= 2);
    if children.len() % 2 != 1 {
        return Err(other_err!(
            "Incorrect parameter count in the call to native function 'JSON_OBJECT'"
        ));
    }
    super::function::validate_expr_return_type(&children[0], EvalType::Json)?;
    for chunk in children[1..].chunks(2) {
        super::function::validate_expr_return_type(&chunk[0], EvalType::Bytes)?;
        super::function::validate_expr_return_type(&chunk[1], EvalType::Json)?;
    }
    Ok(())
}

#[rpn_fn(nullable, varg)]
#[inline]
fn json_array(args: &[Option<JsonRef>]) -> Result<Option<Json>> {
    let mut jsons = vec![];
    for arg in args {
        match arg {
            None => jsons.push(Json::none()?),
            Some(j) => jsons.push((*j).to_owned()),
        }
    }
    Ok(Some(Json::from_array(jsons)?))
}

fn json_object_validator(expr: &tipb::Expr) -> Result<()> {
    let chunks = expr.get_children();
    if chunks.len() % 2 == 1 {
        return Err(other_err!(
            "Incorrect parameter count in the call to native function 'JSON_OBJECT'"
        ));
    }
    for chunk in chunks.chunks(2) {
        super::function::validate_expr_return_type(&chunk[0], EvalType::Bytes)?;
        super::function::validate_expr_return_type(&chunk[1], EvalType::Json)?;
    }
    Ok(())
}

/// Required args like `&[(Option<&Byte>, Option<JsonRef>)]`.
#[rpn_fn(nullable, raw_varg, extra_validator = json_object_validator)]
#[inline]
fn json_object(raw_args: &[ScalarValueRef]) -> Result<Option<Json>> {
    let mut pairs = BTreeMap::new();
    for chunk in raw_args.chunks(2) {
        assert_eq!(chunk.len(), 2);
        let key: Option<BytesRef> = chunk[0].as_bytes();
        if key.is_none() {
            return Err(other_err!(
                "Data truncation: JSON documents may not contain NULL member names."
            ));
        }
        let key = String::from_utf8(key.unwrap().to_owned())
            .map_err(tidb_query_datatype::codec::Error::from)?;

        let value: Option<JsonRef> = chunk[1].as_json();
        let value = match value {
            None => Json::none()?,
            Some(v) => v.to_owned(),
        };

        pairs.insert(key, value);
    }
    Ok(Some(Json::from_object(pairs)?))
}

// According to mysql 5.7,
// arguments of json_merge should not be less than 2.
#[rpn_fn(nullable, varg, min_args = 2)]
#[inline]
pub fn json_merge(args: &[Option<JsonRef>]) -> Result<Option<Json>> {
    // min_args = 2, so it's ok to call args[0]
    if args[0].is_none() {
        return Ok(None);
    }
    let mut jsons: Vec<JsonRef> = vec![];
    let json_none = Json::none()?;
    for arg in args {
        match arg {
            None => jsons.push(json_none.as_ref()),
            Some(j) => jsons.push(*j),
        }
    }
    Ok(Some(Json::merge(jsons)?))
}

// `json_merge_patch` is the implementation for JSON_MERGE_PATCH in mysql
// <https://dev.mysql.com/doc/refman/8.3/en/json-modification-functions.html#function_json-merge-patch>
//
// The json_merge_patch rules are listed as following:
// 1. If the first argument is not an object, the result of the merge is the
//    same as if an empty object had been merged with the second argument.
// 2. If the second argument is not an object, the result of the merge is the
//    second argument.
// 3. If both arguments are objects, the result of the merge is an object with
//    the following members: 3.1. All members of the first object which do not
//    have a corresponding member with the same key in the second object. 3.2.
//    All members of the second object which do not have a corresponding key in
//    the first object, and whose value is not the JSON null literal. 3.3. All
//    members with a key that exists in both the first and the second object,
//    and whose value in the second object is not the JSON null literal. The
//    values of these members are the results of recursively merging the value
//    in the first object with the value in the second object.
// See `MergePatchBinaryJSON()` in TiDB
// `pkg/types/json_binary_functions.go`
// arguments of json_merge_patch should not be less than 2.
#[rpn_fn(nullable, varg, min_args = 2)]
#[inline]
pub fn json_merge_patch(args: &[Option<JsonRef>]) -> Result<Option<Json>> {
    let mut jsons: Vec<Option<JsonRef>> = vec![];
    let mut index = 0;
    // according to the implements of RFC7396
    // when the last item is not object
    // we can return the last item directly
    for i in (0..=args.len() - 1).rev() {
        if args[i].is_none() || args[i].unwrap().get_type() != JsonType::Object {
            index = i;
            break;
        }
    }

    if args[index].is_none() {
        return Ok(None);
    }

    jsons.extend(&args[index..]);
    let mut target = jsons[0].unwrap().to_owned();

    if jsons.len() > 1 {
        for i in 1..jsons.len() {
            target = Json::merge_patch(target.as_ref(), jsons[i].unwrap())?;
        }
    }
    Ok(Some(target.to_owned()))
}

#[rpn_fn(writer)]
#[inline]
fn json_quote(input: BytesRef, writer: BytesWriter) -> Result<BytesGuard> {
    Ok(writer.write(quote(input)?))
}

fn quote(bytes: BytesRef) -> Result<Option<Bytes>> {
    let mut result = Vec::with_capacity(bytes.len() * 2 + 2);
    result.push(b'\"');
    for byte in bytes.iter() {
        if *byte == b'\"' || *byte == b'\\' {
            result.push(b'\\');
            result.push(*byte)
        } else if *byte == b'\x07' {
            // \a alert
            result.push(b'\\');
            result.push(b'a');
        } else if *byte == b'\x08' {
            // \b backspace
            result.push(b'\\');
            result.push(b'b')
        } else if *byte == b'\x0c' {
            // \f form feed
            result.push(b'\\');
            result.push(b'f')
        } else if *byte == b'\n' {
            result.push(b'\\');
            result.push(b'n');
        } else if *byte == b'\r' {
            result.push(b'\\');
            result.push(b'r');
        } else if *byte == b'\t' {
            result.push(b'\\');
            result.push(b't')
        } else if *byte == b'\x0b' {
            // \v vertical tab
            result.push(b'\\');
            result.push(b'v')
        } else {
            result.push(*byte)
        }
    }
    result.push(b'\"');
    Ok(Some(result))
}

#[rpn_fn(nullable, raw_varg, min_args = 1, max_args = 1)]
#[inline]
fn json_valid(args: &[ScalarValueRef]) -> Result<Option<Int>> {
    assert_eq!(args.len(), 1);
    let received_et = args[0].eval_type();
    let r = match args[0].to_owned().is_none() {
        true => None,
        _ => match received_et {
            EvalType::Json => args[0].as_json().and(Some(1)),
            EvalType::Bytes => match args[0].as_bytes() {
                Some(p) => {
                    let tmp_str =
                        std::str::from_utf8(p).map_err(tidb_query_datatype::codec::Error::from)?;
                    let json: serde_json::error::Result<Json> = serde_json::from_str(tmp_str);
                    Some(json.is_ok() as Int)
                }
                _ => Some(0),
            },
            _ => Some(0),
        },
    };

    Ok(r)
}

#[rpn_fn]
#[inline]
fn json_unquote(arg: BytesRef) -> Result<Option<Bytes>> {
    let tmp_str = std::str::from_utf8(arg)?;
    let first_char = tmp_str.chars().next();
    let last_char = tmp_str.chars().last();
    if tmp_str.len() >= 2 && first_char == Some('"') && last_char == Some('"') {
        let _: IgnoredAny = serde_json::from_str(tmp_str)?;
    }
    Ok(Some(Bytes::from(self::unquote_string(tmp_str)?)))
}

// Args should be like `(Option<JsonRef> , &[Option<BytesRef>])`.
fn json_with_paths_validator(expr: &tipb::Expr) -> Result<()> {
    assert!(expr.get_children().len() >= 2);
    // args should be like `Option<JsonRef> , &[Option<BytesRef>]`.
    valid_paths(expr)
}

fn valid_paths(expr: &tipb::Expr) -> Result<()> {
    let children = expr.get_children();
    super::function::validate_expr_return_type(&children[0], EvalType::Json)?;
    for child in children.iter().skip(1) {
        super::function::validate_expr_return_type(child, EvalType::Bytes)?;
    }
    Ok(())
}

fn unquote_string(s: &str) -> Result<String> {
    let first_char = s.chars().next();
    let last_char = s.chars().last();
    if s.len() >= 2 && first_char == Some('"') && last_char == Some('"') {
        Ok(json_unquote::unquote_string(&s[1..s.len() - 1])?)
    } else {
        Ok(String::from(s))
    }
}

#[rpn_fn(nullable, raw_varg, min_args = 2, extra_validator = json_with_paths_validator)]
#[inline]
fn json_extract(args: &[ScalarValueRef]) -> Result<Option<Json>> {
    assert!(args.len() >= 2);
    let j: Option<JsonRef> = args[0].as_json();
    let j = match j {
        None => return Ok(None),
        Some(j) => j.to_owned(),
    };

    let path_expr_list = try_opt!(parse_json_path_list(&args[1..]));

    Ok(j.as_ref().extract(&path_expr_list)?)
}

// Args should be like `(Option<JsonRef> , &[Option<BytesRef>])`.
fn json_with_path_validator(expr: &tipb::Expr) -> Result<()> {
    assert!(expr.get_children().len() == 2 || expr.get_children().len() == 1);
    valid_paths(expr)
}

#[rpn_fn(nullable, raw_varg,min_args= 1, max_args = 2, extra_validator = json_with_path_validator)]
#[inline]
fn json_keys(args: &[ScalarValueRef]) -> Result<Option<Json>> {
    assert!(!args.is_empty() && args.len() <= 2);
    if let Some(j) = args[0].as_json() {
        if let Some(list) = parse_json_path_list(&args[1..])? {
            return Ok(j.keys(&list)?);
        }
    }
    Ok(None)
}

#[rpn_fn(nullable, raw_varg,min_args= 1, max_args = 2, extra_validator = json_with_path_validator)]
#[inline]
fn json_length(args: &[ScalarValueRef]) -> Result<Option<Int>> {
    assert!(!args.is_empty() && args.len() <= 2);
    let j: Option<JsonRef> = args[0].as_json();
    let j = match j {
        None => return Ok(None),
        Some(j) => j.to_owned(),
    };
    Ok(match parse_json_path_list(&args[1..])? {
        Some(path_expr_list) => j.as_ref().json_length(&path_expr_list)?,
        None => None,
    })
}

// Args should be like `(Option<JsonRef> , Option<JsonRef>,
// &[Option<BytesRef>])`. or `(Option<JsonRef> , Option<JsonRef>)`
fn json_contains_validator(expr: &tipb::Expr) -> Result<()> {
    assert!(expr.get_children().len() == 2 || expr.get_children().len() == 3);
    let children = expr.get_children();
    super::function::validate_expr_return_type(&children[0], EvalType::Json)?;
    super::function::validate_expr_return_type(&children[1], EvalType::Json)?;
    if expr.get_children().len() == 3 {
        super::function::validate_expr_return_type(&children[2], EvalType::Bytes)?;
    }
    Ok(())
}

#[rpn_fn(nullable, raw_varg,min_args= 2, max_args = 3, extra_validator = json_contains_validator)]
#[inline]
fn json_contains(args: &[ScalarValueRef]) -> Result<Option<i64>> {
    assert!(args.len() == 2 || args.len() == 3);
    let j: Option<JsonRef> = args[0].as_json();
    let mut j = match j {
        None => return Ok(None),
        Some(j) => j.to_owned(),
    };
    let target: Option<JsonRef> = args[1].as_json();
    let target = match target {
        None => return Ok(None),
        Some(target) => target,
    };

    if args.len() == 3 {
        match parse_json_path_list(&args[2..])? {
            Some(path_expr_list) => {
                if path_expr_list.len() == 1 && path_expr_list[0].contains_any_asterisk() {
                    return Ok(None);
                }
                match j.as_ref().extract(&path_expr_list)? {
                    Some(json) => {
                        j = json;
                    }
                    _ => return Ok(None),
                }
            }
            None => return Ok(None),
        };
    }
    Ok(Some(j.as_ref().json_contains(target)? as i64))
}

// Args should be like `(Option<JsonRef> , Option<JsonRef>)`
fn member_of_validator(expr: &tipb::Expr) -> Result<()> {
    assert!(expr.get_children().len() == 2);
    let children = expr.get_children();
    super::function::validate_expr_return_type(&children[0], EvalType::Json)?;
    super::function::validate_expr_return_type(&children[1], EvalType::Json)?;
    Ok(())
}

#[rpn_fn(nullable, raw_varg,min_args= 2, max_args = 2, extra_validator = member_of_validator)]
#[inline]
fn member_of(args: &[ScalarValueRef]) -> Result<Option<i64>> {
    assert!(args.len() == 2);
    let value: Option<JsonRef> = args[0].as_json();
    let value = match value {
        None => return Ok(None),
        Some(value) => value.to_owned(),
    };

    let json_array: Option<JsonRef> = args[1].as_json();
    let json_array = match json_array {
        None => return Ok(None),
        Some(json_array) => json_array,
    };

    Ok(Some(value.as_ref().member_of(json_array)? as i64))
}

#[rpn_fn(nullable, raw_varg, min_args = 2, extra_validator = json_with_paths_validator)]
#[inline]
fn json_remove(args: &[ScalarValueRef]) -> Result<Option<Json>> {
    assert!(args.len() >= 2);
    let j: Option<JsonRef> = args[0].as_json();
    let j = match j {
        None => return Ok(None),
        Some(j) => j.to_owned(),
    };

    let path_expr_list = try_opt!(parse_json_path_list(&args[1..]));

    Ok(Some(j.as_ref().remove(&path_expr_list)?))
}

fn parse_json_path_list(args: &[ScalarValueRef]) -> Result<Option<Vec<PathExpression>>> {
    let mut path_expr_list = Vec::with_capacity(args.len());
    for arg in args {
        let json_path: Option<BytesRef> = arg.as_bytes();

        path_expr_list.push(try_opt!(parse_json_path(json_path)));
    }
    Ok(Some(path_expr_list))
}

#[inline]
fn parse_json_path(path: Option<BytesRef>) -> Result<Option<PathExpression>> {
    let json_path = match path {
        None => return Ok(None),
        Some(p) => std::str::from_utf8(p).map_err(tidb_query_datatype::codec::Error::from),
    }?;

    Ok(Some(parse_json_path_expr(json_path)?))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use tipb::ScalarFuncSig;

    use super::*;
    use crate::types::test_util::RpnFnScalarEvaluator;

    #[test]
    fn test_json_depth() {
        let cases = vec![
            (None, None),
            (Some("null"), Some(1)),
            (Some("[true, 2017]"), Some(2)),
            (
                Some(r#"{"a": {"a1": [3]}, "b": {"b1": {"c": {"d": [5]}}}}"#),
                Some(6),
            ),
            (Some("{}"), Some(1)),
            (Some("[]"), Some(1)),
            (Some("true"), Some(1)),
            (Some("1"), Some(1)),
            (Some("-1"), Some(1)),
            (Some(r#""a""#), Some(1)),
            (Some(r#"[10, 20]"#), Some(2)),
            (Some(r#"[[], {}]"#), Some(2)),
            (Some(r#"[10, {"a": 20}]"#), Some(3)),
            (Some(r#"[[2], 3, [[[4]]]]"#), Some(5)),
            (Some(r#"{"Name": "Homer"}"#), Some(2)),
            (Some(r#"[10, {"a": 20}]"#), Some(3)),
            (
                Some(
                    r#"{"Person": {"Name": "Homer", "Age": 39, "Hobbies": ["Eating", "Sleeping"]} }"#,
                ),
                Some(4),
            ),
            (Some(r#"{"a":1}"#), Some(2)),
            (Some(r#"{"a":[1]}"#), Some(3)),
            (Some(r#"{"b":2, "c":3}"#), Some(2)),
            (Some(r#"[1]"#), Some(2)),
            (Some(r#"[1,2]"#), Some(2)),
            (Some(r#"[1,2,[1,3]]"#), Some(3)),
            (Some(r#"[1,2,[1,[5,[3]]]]"#), Some(5)),
            (Some(r#"[1,2,[1,[5,{"a":[2,3]}]]]"#), Some(6)),
            (Some(r#"[{"a":1}]"#), Some(3)),
            (Some(r#"[{"a":1,"b":2}]"#), Some(3)),
            (Some(r#"[{"a":{"a":1},"b":2}]"#), Some(4)),
        ];
        for (arg, expect_output) in cases {
            let arg = arg.map(|input| Json::from_str(input).unwrap());

            let output = RpnFnScalarEvaluator::new()
                .push_param(arg.clone())
                .evaluate(ScalarFuncSig::JsonDepthSig)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}", arg);
        }
    }

    #[test]
    fn test_json_type() {
        let cases = vec![
            (None, None),
            (Some(r#"true"#), Some("BOOLEAN")),
            (Some(r#"null"#), Some("NULL")),
            (Some(r#"-3"#), Some("INTEGER")),
            (Some(r#"3"#), Some("INTEGER")),
            (Some(r#"9223372036854775808"#), Some("UNSIGNED INTEGER")),
            (Some(r#"3.14"#), Some("DOUBLE")),
            (Some(r#"[1, 2, 3]"#), Some("ARRAY")),
            (Some(r#"{"name": 123}"#), Some("OBJECT")),
        ];

        for (arg, expect_output) in cases {
            let arg = arg.map(|input| Json::from_str(input).unwrap());
            let expect_output = expect_output.map(Bytes::from);

            let output = RpnFnScalarEvaluator::new()
                .push_param(arg.clone())
                .evaluate(ScalarFuncSig::JsonTypeSig)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}", arg);
        }
    }

    #[test]
    fn test_json_modify() {
        let cases: Vec<(_, Vec<ScalarValue>, _)> = vec![
            (
                ScalarFuncSig::JsonSetSig,
                vec![
                    None::<Json>.into(),
                    None::<Bytes>.into(),
                    None::<Json>.into(),
                ],
                None::<Json>,
            ),
            (
                ScalarFuncSig::JsonSetSig,
                vec![
                    Some(Json::from_i64(9).unwrap()).into(),
                    Some(b"$[1]".to_vec()).into(),
                    Some(Json::from_u64(3).unwrap()).into(),
                ],
                Some(r#"[9,3]"#.parse().unwrap()),
            ),
            (
                ScalarFuncSig::JsonInsertSig,
                vec![
                    Some(Json::from_i64(9).unwrap()).into(),
                    Some(b"$[1]".to_vec()).into(),
                    Some(Json::from_u64(3).unwrap()).into(),
                ],
                Some(r#"[9,3]"#.parse().unwrap()),
            ),
            (
                ScalarFuncSig::JsonReplaceSig,
                vec![
                    Some(Json::from_i64(9).unwrap()).into(),
                    Some(b"$[1]".to_vec()).into(),
                    Some(Json::from_u64(3).unwrap()).into(),
                ],
                Some(r#"9"#.parse().unwrap()),
            ),
            (
                ScalarFuncSig::JsonSetSig,
                vec![
                    Some(Json::from_str(r#"{"a":"x"}"#).unwrap()).into(),
                    Some(b"$.a".to_vec()).into(),
                    None::<Json>.into(),
                ],
                Some(r#"{"a":null}"#.parse().unwrap()),
            ),
        ];
        for (sig, args, expect_output) in cases {
            let output: Option<Json> = RpnFnScalarEvaluator::new()
                .push_params(args.clone())
                .evaluate(sig)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}", args);
        }
    }

    #[test]
    fn test_json_array() {
        let cases = vec![
            (vec![], Some(r#"[]"#)),
            (vec![Some(r#"1"#), None], Some(r#"[1, null]"#)),
            (
                vec![
                    Some(r#"1"#),
                    None,
                    Some(r#"2"#),
                    Some(r#""sdf""#),
                    Some(r#""k1""#),
                    Some(r#""v1""#),
                ],
                Some(r#"[1, null, 2, "sdf", "k1", "v1"]"#),
            ),
        ];

        for (vargs, expected) in cases {
            let vargs = vargs
                .into_iter()
                .map(|input| input.map(|s| Json::from_str(s).unwrap()))
                .collect::<Vec<_>>();
            let expected = expected.map(|s| Json::from_str(s).unwrap());

            let output = RpnFnScalarEvaluator::new()
                .push_params(vargs.clone())
                .evaluate(ScalarFuncSig::JsonArraySig)
                .unwrap();
            assert_eq!(output, expected, "{:?}", vargs);
        }
    }

    #[test]
    fn test_json_merge() {
        let cases = vec![
            (vec![None, None], None),
            (vec![Some("{}"), Some("[]")], Some("[{}]")),
            (
                vec![Some(r#"{}"#), Some(r#"[]"#), Some(r#"3"#), Some(r#""4""#)],
                Some(r#"[{}, 3, "4"]"#),
            ),
            (
                vec![Some("[1, 2]"), Some("[3, 4]")],
                Some(r#"[1, 2, 3, 4]"#),
            ),
        ];

        for (vargs, expected) in cases {
            let vargs = vargs
                .into_iter()
                .map(|input| input.map(|s| Json::from_str(s).unwrap()))
                .collect::<Vec<_>>();
            let expected = expected.map(|s| Json::from_str(s).unwrap());

            let output = RpnFnScalarEvaluator::new()
                .push_params(vargs.clone())
                .evaluate(ScalarFuncSig::JsonMergeSig)
                .unwrap();
            assert_eq!(output, expected, "{:?}", vargs);
        }
    }

    #[test]
    fn test_json_object() {
        let cases = vec![
            (vec![], r#"{}"#),
            (vec![("1", None)], r#"{"1":null}"#),
            (
                vec![
                    ("1", None),
                    ("2", Some(r#""sdf""#)),
                    ("k1", Some(r#""v1""#)),
                ],
                r#"{"1":null,"2":"sdf","k1":"v1"}"#,
            ),
        ];

        for (vargs, expected) in cases {
            let mut new_vargs: Vec<ScalarValue> = vec![];
            for (key, value) in vargs
                .into_iter()
                .map(|(key, value)| (Bytes::from(key), value.map(|s| Json::from_str(s).unwrap())))
            {
                new_vargs.push(ScalarValue::from(key));
                new_vargs.push(ScalarValue::from(value));
            }

            let expected = Json::from_str(expected).unwrap();

            let output: Json = RpnFnScalarEvaluator::new()
                .push_params(new_vargs)
                .evaluate(ScalarFuncSig::JsonObjectSig)
                .unwrap()
                .unwrap();
            assert_eq!(output, expected);
        }

        let err_cases = vec![
            vec![
                ScalarValue::from(Bytes::from("1")),
                ScalarValue::from(None::<Json>),
                ScalarValue::from(Bytes::from("1")),
            ],
            vec![
                ScalarValue::from(None::<Bytes>),
                ScalarValue::from(Json::from_str("1").unwrap()),
            ],
        ];

        for err_args in err_cases {
            let output: Result<Option<Json>> = RpnFnScalarEvaluator::new()
                .push_params(err_args)
                .evaluate(ScalarFuncSig::JsonObjectSig);

            output.unwrap_err();
        }
    }

    #[test]
    fn test_json_quote() {
        let cases = vec![
            (None, None),
            (Some(""), Some(r#""""#)),
            (Some(r#""""#), Some(r#""\"\"""#)),
            (Some(r#"a"#), Some(r#""a""#)),
            (Some(r#"3"#), Some(r#""3""#)),
            (Some(r#"{"a": "b"}"#), Some(r#""{\"a\": \"b\"}""#)),
            (Some(r#"{"a":     "b"}"#), Some(r#""{\"a\":     \"b\"}""#)),
            (
                Some(r#"hello,"quoted string",world"#),
                Some(r#""hello,\"quoted string\",world""#),
            ),
            (
                Some(r#"hello,"宽字符",world"#),
                Some(r#""hello,\"宽字符\",world""#),
            ),
            (
                Some(r#""Invalid Json string	is OK"#),
                Some(r#""\"Invalid Json string\tis OK""#),
            ),
            (Some(r#"1\u2232\u22322"#), Some(r#""1\\u2232\\u22322""#)),
            (
                Some("new line \"\r\n\" is ok"),
                Some(r#""new line \"\r\n\" is ok""#),
            ),
        ];

        for (arg, expect_output) in cases {
            let arg = arg.map(Bytes::from);
            let expect_output = expect_output.map(Bytes::from);

            let output = RpnFnScalarEvaluator::new()
                .push_param(arg.clone())
                .evaluate(ScalarFuncSig::JsonQuoteSig)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}", arg);
        }
    }

    #[test]
    fn test_json_unquote() {
        let cases = vec![
            (None, None, true),
            (Some(r#"""#), Some(r#"""#), true),
            (Some(r"a"), Some("a"), true),
            (Some(r#""3"#), Some(r#""3"#), true),
            (Some(r#"{"a":  "b"}"#), Some(r#"{"a":  "b"}"#), true),
            (
                Some(r#""hello,\"quoted string\",world""#),
                Some(r#"hello,"quoted string",world"#),
                true,
            ),
            (Some(r#"A中\\\"文B"#), Some(r#"A中\\\"文B"#), true),
            (Some(r#""A中\\\"文B""#), Some(r#"A中\"文B"#), true),
            (Some(r#""\u00E0A中\\\"文B""#), Some(r#"àA中\"文B"#), true),
            (Some(r#""a""#), Some(r#"a"#), true),
            (Some(r#"""a"""#), None, false),
            (Some(r#""""a""""#), None, false),
        ];

        for (arg, expect, success) in cases {
            let arg = arg.map(Bytes::from);
            let expect = expect.map(Bytes::from);
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg.clone())
                .evaluate(ScalarFuncSig::JsonUnquoteSig);
            match output {
                Ok(s) => {
                    assert_eq!(s, expect, "{:?}", arg);
                    assert_eq!(success, true);
                }
                Err(_) => {
                    assert_eq!(success, false);
                }
            }
        }
    }

    #[test]
    fn test_json_extract() {
        let cases: Vec<(Vec<ScalarValue>, _)> = vec![
            (vec![None::<Json>.into(), None::<Bytes>.into()], None),
            (
                vec![
                    Some(Json::from_str("[10, 20, [30, 40]]").unwrap()).into(),
                    Some(b"$[1]".to_vec()).into(),
                ],
                Some("20"),
            ),
            (
                vec![
                    Some(Json::from_str("[10, 20, [30, 40]]").unwrap()).into(),
                    Some(b"$[1]".to_vec()).into(),
                    Some(b"$[0]".to_vec()).into(),
                ],
                Some("[20, 10]"),
            ),
            (
                vec![
                    Some(Json::from_str("[10, 20, [30, 40]]").unwrap()).into(),
                    Some(b"$[2][*]".to_vec()).into(),
                ],
                Some("[30, 40]"),
            ),
            (
                vec![
                    Some(Json::from_str("[10, 20, [30, 40]]").unwrap()).into(),
                    Some(b"$[2][*]".to_vec()).into(),
                    None::<Bytes>.into(),
                ],
                None,
            ),
        ];

        for (vargs, expected) in cases {
            let expected = expected.map(|s| Json::from_str(s).unwrap());

            let output = RpnFnScalarEvaluator::new()
                .push_params(vargs.clone())
                .evaluate(ScalarFuncSig::JsonExtractSig)
                .unwrap();
            assert_eq!(output, expected, "{:?}", vargs);
        }
    }

    #[test]
    fn test_json_remove() {
        let cases: Vec<(Vec<ScalarValue>, _)> = vec![(
            vec![
                Some(Json::from_str(r#"["a", ["b", "c"], "d"]"#).unwrap()).into(),
                Some(b"$[1]".to_vec()).into(),
            ],
            Some(r#"["a", "d"]"#),
        )];

        for (vargs, expected) in cases {
            let expected = expected.map(|s| Json::from_str(s).unwrap());

            let output = RpnFnScalarEvaluator::new()
                .push_params(vargs.clone())
                .evaluate(ScalarFuncSig::JsonRemoveSig)
                .unwrap();
            assert_eq!(output, expected, "{:?}", vargs);
        }
    }

    #[test]
    fn test_json_length() {
        let cases: Vec<(Vec<ScalarValue>, Option<i64>)> = vec![
            (
                vec![
                    Some(Json::from_str("null").unwrap()).into(),
                    None::<Bytes>.into(),
                ],
                None,
            ),
            (
                vec![
                    Some(Json::from_str("false").unwrap()).into(),
                    None::<Bytes>.into(),
                ],
                None,
            ),
            (vec![Some(Json::from_str("1").unwrap()).into()], Some(1)),
            (
                vec![
                    Some(Json::from_str(r#"{"a": [1, 2, {"aa": "xx"}]}"#).unwrap()).into(),
                    Some(b"$.*".to_vec()).into(),
                ],
                None,
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a":{"a":1},"b":2}"#).unwrap()).into(),
                    Some(b"$".to_vec()).into(),
                ],
                Some(2),
            ),
        ];

        for (vargs, expected) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(vargs.clone())
                .evaluate(ScalarFuncSig::JsonLengthSig)
                .unwrap();
            assert_eq!(output, expected, "{:?}", vargs);
        }
    }

    #[test]
    fn test_json_valid() {
        let cases: Vec<(Vec<ScalarValue>, Option<i64>)> = vec![
            (
                vec![Some(Json::from_str(r#"{"a":1}"#).unwrap()).into()],
                Some(1),
            ),
            (vec![Some(b"hello".to_vec()).into()], Some(0)),
            (vec![Some(b"\"hello\"".to_vec()).into()], Some(1)),
            (vec![Some(b"null".to_vec()).into()], Some(1)),
            (vec![Some(Json::from_str(r#"{}"#).unwrap()).into()], Some(1)),
            (vec![Some(Json::from_str(r#"[]"#).unwrap()).into()], Some(1)),
            (vec![Some(b"2".to_vec()).into()], Some(1)),
            (vec![Some(b"2.5".to_vec()).into()], Some(1)),
            (vec![Some(b"2019-8-19".to_vec()).into()], Some(0)),
            (vec![Some(b"\"2019-8-19\"".to_vec()).into()], Some(1)),
            (vec![Some(2).into()], Some(0)),
            (vec![Some(2.5).into()], Some(0)),
            (vec![None::<Json>.into()], None),
            (vec![None::<Bytes>.into()], None),
            (vec![None::<Int>.into()], None),
        ];

        for (vargs, expected) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(vargs.clone())
                .evaluate(ScalarFuncSig::JsonValidJsonSig)
                .unwrap();
            assert_eq!(output, expected, "{:?}", vargs);
        }
    }

    #[test]
    fn test_json_contains() {
        let cases: Vec<(Vec<ScalarValue>, Option<i64>)> = vec![
            (
                vec![
                    Some(Json::from_str(r#"{"a":{"a":1},"b":2}"#).unwrap()).into(),
                    Some(Json::from_str(r#"2"#).unwrap()).into(),
                    Some(b"$.b".to_vec()).into(),
                ],
                Some(1),
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a":{"a":1},"b":2}"#).unwrap()).into(),
                    Some(Json::from_str(r#"3"#).unwrap()).into(),
                    Some(b"$.b".to_vec()).into(),
                ],
                Some(0),
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a":{"a":1},"b":2}"#).unwrap()).into(),
                    Some(Json::from_str(r#"{"b":3}"#).unwrap()).into(),
                ],
                Some(0),
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a":{"a":1},"b":2}"#).unwrap()).into(),
                    Some(Json::from_str(r#"{"b":2}"#).unwrap()).into(),
                ],
                Some(1),
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a":{"a":1},"b":2}"#).unwrap()).into(),
                    Some(Json::from_str(r#"{"a":1}"#).unwrap()).into(),
                    Some(b"$.a".to_vec()).into(),
                ],
                Some(1),
            ),
            (
                vec![
                    Some(Json::from_str(r#"[{"optUid": 10, "value": "admin"}]"#).unwrap()).into(),
                    Some(Json::from_str(r#"10"#).unwrap()).into(),
                    Some(b"$[0].optUid".to_vec()).into(),
                ],
                Some(1),
            ),
            // copy from tidb  Tests None arguments
            (vec![None::<Json>.into(), None::<Json>.into()], None),
            (
                vec![
                    Some(Json::from_str(r#"{"a":1}"#).unwrap()).into(),
                    None::<Json>.into(),
                ],
                None,
            ),
            (
                vec![
                    None::<Json>.into(),
                    Some(Json::from_str(r#"1"#).unwrap()).into(),
                ],
                None,
            ),
            (
                vec![
                    None::<Json>.into(),
                    Some(Json::from_str(r#"1"#).unwrap()).into(),
                    Some(b"$.c".to_vec()).into(),
                ],
                None,
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a": [1, 2, {"aa": "xx"}]}"#).unwrap()).into(),
                    None::<Json>.into(),
                    Some(b"$.a[3]".to_vec()).into(),
                ],
                None,
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a": [1, 2, {"aa": "xx"}]}"#).unwrap()).into(),
                    Some(Json::from_str(r#"1"#).unwrap()).into(),
                    None::<Bytes>.into(),
                ],
                None,
            ),
            //  Tests with path expression
            (
                vec![
                    Some(Json::from_str(r#"[1,2,[1,[5,[3]]]]"#).unwrap()).into(),
                    Some(Json::from_str(r#"[1,3]"#).unwrap()).into(),
                    Some(b"$[2]".to_vec()).into(),
                ],
                Some(1),
            ),
            (
                vec![
                    Some(Json::from_str(r#"[1,2,[1,[5,{"a":[2,3]}]]]"#).unwrap()).into(),
                    Some(Json::from_str(r#"[1,{"a":[3]}]"#).unwrap()).into(),
                    Some(b"$[2]".to_vec()).into(),
                ],
                Some(1),
            ),
            (
                vec![
                    Some(Json::from_str(r#"[{"a":1}]"#).unwrap()).into(),
                    Some(Json::from_str(r#"{"a":1}"#).unwrap()).into(),
                    Some(b"$".to_vec()).into(),
                ],
                Some(1),
            ),
            (
                vec![
                    Some(Json::from_str(r#"[{"a":1,"b":2}]"#).unwrap()).into(),
                    Some(Json::from_str(r#"{"a":1,"b":2}"#).unwrap()).into(),
                    Some(b"$".to_vec()).into(),
                ],
                Some(1),
            ),
            (
                vec![
                    Some(Json::from_str(r#"[{"a":{"a":1},"b":2}]"#).unwrap()).into(),
                    Some(Json::from_str(r#"{"a":1}"#).unwrap()).into(),
                    Some(b"$.a".to_vec()).into(),
                ],
                None,
            ),
            // Tests without path expression
            // 		{[]interface{}{`{}`, `{}`}, 1, nil},
            // 		{[]interface{}{`{"a":1}`, `{}`}, 1, nil},
            // 		{[]interface{}{`{"a":1}`, `1`}, 0, nil},
            (
                vec![
                    Some(Json::from_str(r#"{}"#).unwrap()).into(),
                    Some(Json::from_str(r#"{}"#).unwrap()).into(),
                ],
                Some(1),
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a":1}"#).unwrap()).into(),
                    Some(Json::from_str(r#"{}"#).unwrap()).into(),
                ],
                Some(1),
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a":1}"#).unwrap()).into(),
                    Some(Json::from_str(r#"1"#).unwrap()).into(),
                ],
                Some(0),
            ),
            // 		{[]interface{}{`{"a":[1]}`, `[1]`}, 0, nil},
            // 		{[]interface{}{`{"b":2, "c":3}`, `{"c":3}`}, 1, nil},
            // 		{[]interface{}{`1`, `1`}, 1, nil},
            // 		{[]interface{}{`[1]`, `1`}, 1, nil},
            (
                vec![
                    Some(Json::from_str(r#"{"a":[1]}"#).unwrap()).into(),
                    Some(Json::from_str(r#"[1]"#).unwrap()).into(),
                ],
                Some(0),
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"b":2, "c":3}"#).unwrap()).into(),
                    Some(Json::from_str(r#"{"c":3}"#).unwrap()).into(),
                ],
                Some(1),
            ),
            (
                vec![
                    Some(Json::from_str(r#"1"#).unwrap()).into(),
                    Some(Json::from_str(r#"1"#).unwrap()).into(),
                ],
                Some(1),
            ),
            (
                vec![
                    Some(Json::from_str(r#"[1]"#).unwrap()).into(),
                    Some(Json::from_str(r#"1"#).unwrap()).into(),
                ],
                Some(1),
            ),
            // 		{[]interface{}{`[1,2]`, `[1]`}, 1, nil},
            // 		{[]interface{}{`[1,2]`, `[1,3]`}, 0, nil},
            // 		{[]interface{}{`[1,2]`, `["1"]`}, 0, nil},
            // 		{[]interface{}{`[1,2,[1,3]]`, `[1,3]`}, 1, nil},
            (
                vec![
                    Some(Json::from_str(r#"[1,2]"#).unwrap()).into(),
                    Some(Json::from_str(r#"[1]"#).unwrap()).into(),
                ],
                Some(1),
            ),
            (
                vec![
                    Some(Json::from_str(r#"[1,2]"#).unwrap()).into(),
                    Some(Json::from_str(r#"[1,3]"#).unwrap()).into(),
                ],
                Some(0),
            ),
            (
                vec![
                    Some(Json::from_str(r#"[1,2]"#).unwrap()).into(),
                    Some(Json::from_str(r#"["1"]"#).unwrap()).into(),
                ],
                Some(0),
            ),
            (
                vec![
                    Some(Json::from_str(r#"[1,2,[1,3]]"#).unwrap()).into(),
                    Some(Json::from_str(r#"[1,3]"#).unwrap()).into(),
                ],
                Some(1),
            ),
            // 		{[]interface{}{`[1,2,[1,3]]`, `[1,      3]`}, 1, nil},
            // 		{[]interface{}{`[1,2,[1,[5,[3]]]]`, `[1,3]`}, 1, nil},
            // 		{[]interface{}{`[1,2,[1,[5,{"a":[2,3]}]]]`, `[1,{"a":[3]}]`}, 1, nil},
            (
                vec![
                    Some(Json::from_str(r#"[1,2,[1,3]]"#).unwrap()).into(),
                    Some(Json::from_str(r#"[1,      3]"#).unwrap()).into(),
                ],
                Some(1),
            ),
            (
                vec![
                    Some(Json::from_str(r#"[1,2,[1,[5,[3]]]]"#).unwrap()).into(),
                    Some(Json::from_str(r#"[1,3]"#).unwrap()).into(),
                ],
                Some(1),
            ),
            (
                vec![
                    Some(Json::from_str(r#"[1,2,[1,[5,{"a":[2,3]}]]]"#).unwrap()).into(),
                    Some(Json::from_str(r#"[1,{"a":[3]}]"#).unwrap()).into(),
                ],
                Some(1),
            ),
            // 		{[]interface{}{`[{"a":1}]`, `{"a":1}`}, 1, nil},
            // 		{[]interface{}{`[{"a":1,"b":2}]`, `{"a":1}`}, 1, nil},
            // 		{[]interface{}{`[{"a":{"a":1},"b":2}]`, `{"a":1}`}, 0, nil},
            (
                vec![
                    Some(Json::from_str(r#"[{"a":1}]"#).unwrap()).into(),
                    Some(Json::from_str(r#"{"a":1}"#).unwrap()).into(),
                ],
                Some(1),
            ),
            (
                vec![
                    Some(Json::from_str(r#"[{"a":1,"b":2}]"#).unwrap()).into(),
                    Some(Json::from_str(r#"{"a":1}"#).unwrap()).into(),
                ],
                Some(1),
            ),
            (
                vec![
                    Some(Json::from_str(r#"[{"a":{"a":1},"b":2}]"#).unwrap()).into(),
                    Some(Json::from_str(r#"{"a":1}"#).unwrap()).into(),
                ],
                Some(0),
            ),
            // Tests path expression contains any asterisk
            //      {[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, `1`, "$.*"}, nil,
            // json.ErrInvalidJSONPathWildcard}, 		{[]interface{}{`{"a": [1, 2, {"aa":
            // "xx"}]}`, `1`, "$[*]"}, nil, json.ErrInvalidJSONPathWildcard},
            // 		{[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, `1`, "$**.a"}, nil,
            // json.ErrInvalidJSONPathWildcard},
            (
                vec![
                    Some(Json::from_str(r#"{"a": [1, 2, {"aa": "xx"}]}"#).unwrap()).into(),
                    Some(Json::from_str(r#"1"#).unwrap()).into(),
                    Some(b"$.*".to_vec()).into(),
                ],
                None,
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a": [1, 2, {"aa": "xx"}]}"#).unwrap()).into(),
                    Some(Json::from_str(r#"1"#).unwrap()).into(),
                    Some(b"$[*]".to_vec()).into(),
                ],
                None,
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a": [1, 2, {"aa": "xx"}]}"#).unwrap()).into(),
                    Some(Json::from_str(r#"1"#).unwrap()).into(),
                    Some(b"$**.a".to_vec()).into(),
                ],
                None,
            ),
            // Tests path expression does not identify a section of the target document
            //      {[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, `1`, "$.c"}, nil, nil},
            // 		{[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, `1`, "$.a[3]"}, nil, nil},
            // 		{[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, `1`, "$.a[2].b"}, nil, nil},
            (
                vec![
                    Some(Json::from_str(r#"{"a": [1, 2, {"aa": "xx"}]}"#).unwrap()).into(),
                    Some(Json::from_str(r#"1"#).unwrap()).into(),
                    Some(b"$.c".to_vec()).into(),
                ],
                None,
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a": [1, 2, {"aa": "xx"}]}"#).unwrap()).into(),
                    Some(Json::from_str(r#"1"#).unwrap()).into(),
                    Some(b"$.a[3]".to_vec()).into(),
                ],
                None,
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a": [1, 2, {"aa": "xx"}]}"#).unwrap()).into(),
                    Some(Json::from_str(r#"1"#).unwrap()).into(),
                    Some(b"$.a[2].b".to_vec()).into(),
                ],
                None,
            ),
        ];

        for (vargs, expected) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(vargs.clone())
                .evaluate(ScalarFuncSig::JsonContainsSig)
                .unwrap();
            assert_eq!(output, expected, "{:?}", vargs);
        }
    }

    #[test]
    fn test_json_keys() {
        let cases: Vec<(Vec<ScalarValue>, Option<Json>, bool)> = vec![
            // Tests nil arguments
            (vec![None::<Json>.into(), None::<Bytes>.into()], None, true),
            (
                vec![None::<Json>.into(), Some(b"$.c".to_vec()).into()],
                None,
                true,
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a": 1}"#).unwrap()).into(),
                    None::<Bytes>.into(),
                ],
                None,
                true,
            ),
            (vec![None::<Json>.into()], None, true),
            // Tests with other type
            (vec![Some(Json::from_str("1").unwrap()).into()], None, true),
            (
                vec![Some(Json::from_str(r#""str""#).unwrap()).into()],
                None,
                true,
            ),
            (
                vec![Some(Json::from_str(r#"true"#).unwrap()).into()],
                None,
                true,
            ),
            (
                vec![Some(Json::from_str("null").unwrap()).into()],
                None,
                true,
            ),
            (
                vec![Some(Json::from_str(r#"[1, 2]"#).unwrap()).into()],
                None,
                true,
            ),
            (
                vec![Some(Json::from_str(r#"["1", "2"]"#).unwrap()).into()],
                None,
                true,
            ),
            // Tests without path expression
            (
                vec![Some(Json::from_str(r#"{}"#).unwrap()).into()],
                Some(Json::from_str("[]").unwrap()),
                true,
            ),
            (
                vec![Some(Json::from_str(r#"{"a": 1}"#).unwrap()).into()],
                Some(Json::from_str(r#"["a"]"#).unwrap()),
                true,
            ),
            (
                vec![Some(Json::from_str(r#"{"a": 1, "b": 2}"#).unwrap()).into()],
                Some(Json::from_str(r#"["a", "b"]"#).unwrap()),
                true,
            ),
            (
                vec![Some(Json::from_str(r#"{"a": {"c": 3}, "b": 2}"#).unwrap()).into()],
                Some(Json::from_str(r#"["a", "b"]"#).unwrap()),
                true,
            ),
            // Tests with path expression
            (
                vec![
                    Some(Json::from_str(r#"{"a": 1}"#).unwrap()).into(),
                    Some(b"$.a".to_vec()).into(),
                ],
                None,
                true,
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a": {"c": 3}, "b": 2}"#).unwrap()).into(),
                    Some(b"$.a".to_vec()).into(),
                ],
                Some(Json::from_str(r#"["c"]"#).unwrap()),
                true,
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a": {"c": 3}, "b": 2}"#).unwrap()).into(),
                    None::<Bytes>.into(),
                ],
                None,
                true,
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a": {"c": 3}, "b": 2}"#).unwrap()).into(),
                    Some(b"$.a.c".to_vec()).into(),
                ],
                None,
                true,
            ),
            // Tests path expression contains any asterisk
            (
                vec![
                    Some(Json::from_str(r#"{}"#).unwrap()).into(),
                    Some(b"$.*".to_vec()).into(),
                ],
                None,
                false,
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a": 1}"#).unwrap()).into(),
                    Some(b"$.*".to_vec()).into(),
                ],
                None,
                false,
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a": {"c": 3}, "b": 2}"#).unwrap()).into(),
                    Some(b"$.*".to_vec()).into(),
                ],
                None,
                false,
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a": {"c": 3}, "b": 2}"#).unwrap()).into(),
                    Some(b"$.a.*".to_vec()).into(),
                ],
                None,
                false,
            ),
            // Tests path expression does not identify a section of the target document
            (
                vec![
                    Some(Json::from_str(r#"{"a": 1}"#).unwrap()).into(),
                    Some(b"$.b".to_vec()).into(),
                ],
                None,
                true,
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a": {"c": 3}, "b": 2}"#).unwrap()).into(),
                    Some(b"$.c".to_vec()).into(),
                ],
                None,
                true,
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a": {"c": 3}, "b": 2}"#).unwrap()).into(),
                    Some(b"$.a.d".to_vec()).into(),
                ],
                None,
                true,
            ),
        ];
        for (vargs, expected, is_success) in cases {
            let output = RpnFnScalarEvaluator::new().push_params(vargs.clone());
            let output = if vargs.len() == 1 {
                output.evaluate(ScalarFuncSig::JsonKeysSig)
            } else {
                output.evaluate(ScalarFuncSig::JsonKeys2ArgsSig)
            };
            if is_success {
                assert_eq!(output.unwrap(), expected, "{:?}", vargs);
            } else {
                output.unwrap_err();
            }
        }
    }

    #[test]
    fn test_json_member_of() {
        let test_cases = vec![
            (Some(r#"1"#), Some(r#"[1,2]"#), Some(1)),
            (Some(r#"1"#), Some(r#"[1]"#), Some(1)),
            (Some(r#"1"#), Some(r#"[0]"#), Some(0)),
            (Some(r#"1"#), Some(r#"[[1]]"#), Some(0)),
            (Some(r#""1""#), Some(r#"[1]"#), Some(0)),
            (Some(r#""1""#), Some(r#"["1"]"#), Some(1)),
            (Some(r#""{\"a\":1}""#), Some(r#"{"a":1}"#), Some(0)),
            (Some(r#""{\"a\":1}""#), Some(r#"[{"a":1}]"#), Some(0)),
            (Some(r#""{\"a\":1}""#), Some(r#"[{"a":1}, 1]"#), Some(0)),
            (Some(r#""{\"a\":1}""#), Some(r#"["{\"a\":1}"]"#), Some(1)),
            (Some(r#""{\"a\":1}""#), Some(r#"["{\"a\":1}",1]"#), Some(1)),
            (Some(r#"1"#), Some(r#"1"#), Some(1)),
            (Some(r#"[4,5]"#), Some(r#"[[3,4],[4,5]]"#), Some(1)),
            (Some(r#""[4,5]""#), Some(r#"[[3,4],"[4,5]"]"#), Some(1)),
            (Some(r#"{"a":1}"#), Some(r#"{"a":1}"#), Some(1)),
            (Some(r#"{"a":1}"#), Some(r#"{"a":1, "b":2}"#), Some(0)),
            (Some(r#"{"a":1}"#), Some(r#"[{"a":1}]"#), Some(1)),
            (Some(r#"{"a":1}"#), Some(r#"{"b": {"a":1}}"#), Some(0)),
            (Some(r#"1"#), Some(r#"1"#), Some(1)),
            (Some(r#"[1,2]"#), Some(r#"[1,2]"#), Some(0)),
            (Some(r#"[1,2]"#), Some(r#"[[1,2]]"#), Some(1)),
            (Some(r#"[[1,2]]"#), Some(r#"[[1,2]]"#), Some(0)),
            (Some(r#"[[1,2]]"#), Some(r#"[[[1,2]]]"#), Some(1)),
            (None, Some(r#"[[[1,2]]]"#), None),
            (Some(r#"[[1,2]]"#), None, None),
            (None, None, None),
        ];
        for (js, value, expected) in test_cases {
            let args: Vec<ScalarValue> = vec![
                js.map(|js| Json::from_str(js).unwrap()).into(),
                value.map(|value| Json::from_str(value).unwrap()).into(),
            ];
            let output = RpnFnScalarEvaluator::new()
                .push_params(args.clone())
                .evaluate(ScalarFuncSig::JsonMemberOfSig)
                .unwrap();
            assert_eq!(output, expected, "{:?}", args);
        }
    }

    #[test]
    fn test_json_array_append() {
        let cases: Vec<(Vec<ScalarValue>, _)> = vec![
            // use exact testcase from TiDB repo
            (
                vec![
                    Some(Json::from_str(r#"{"a": 1, "b": [2, 3], "c": 4}"#).unwrap()).into(),
                    Some(b"$.d".to_vec()).into(),
                    Some(Json::from_str(r#""z""#).unwrap()).into(),
                ],
                Some(r#"{"a": 1, "b": [2, 3], "c": 4}"#.parse().unwrap()),
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a": 1, "b": [2, 3], "c": 4}"#).unwrap()).into(),
                    Some(b"$".to_vec()).into(),
                    Some(Json::from_str(r#""w""#).unwrap()).into(),
                ],
                Some(r#"[{"a": 1, "b": [2, 3], "c": 4}, "w"]"#.parse().unwrap()),
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a": 1, "b": [2, 3], "c": 4}"#).unwrap()).into(),
                    Some(b"$".to_vec()).into(),
                    None::<Json>.into(),
                ],
                Some(r#"[{"a": 1, "b": [2, 3], "c": 4}, null]"#.parse().unwrap()),
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a": 1}"#).unwrap()).into(),
                    Some(b"$".to_vec()).into(),
                    Some(Json::from_str(r#"{"b": 2}"#).unwrap()).into(),
                ],
                Some(r#"[{"a": 1}, {"b": 2}]"#.parse().unwrap()),
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a": 1}"#).unwrap()).into(),
                    Some(b"$".to_vec()).into(),
                    Some(Json::from_str(r#"{"b": 2}"#).unwrap()).into(),
                ],
                Some(r#"[{"a": 1}, {"b": 2}]"#.parse().unwrap()),
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a": 1}"#).unwrap()).into(),
                    Some(b"$.a".to_vec()).into(),
                    Some(Json::from_str(r#"{"b": 2}"#).unwrap()).into(),
                ],
                Some(r#"{"a": [1, {"b": 2}]}"#.parse().unwrap()),
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a": 1}"#).unwrap()).into(),
                    Some(b"$.a".to_vec()).into(),
                    Some(Json::from_str(r#"{"b": 2}"#).unwrap()).into(),
                    Some(b"$.a[1]".to_vec()).into(),
                    Some(Json::from_str(r#"{"b": 2}"#).unwrap()).into(),
                ],
                Some(r#"{"a": [1, [{"b": 2}, {"b": 2}]]}"#.parse().unwrap()),
            ),
            (
                vec![
                    None::<Json>.into(),
                    Some(b"$".to_vec()).into(),
                    None::<Json>.into(),
                ],
                None::<Json>,
            ),
            (
                vec![
                    None::<Json>.into(),
                    Some(b"$".to_vec()).into(),
                    Some(Json::from_str(r#""a""#).unwrap()).into(),
                ],
                None::<Json>,
            ),
            (
                vec![
                    Some(Json::from_str(r#"null"#).unwrap()).into(),
                    Some(b"$".to_vec()).into(),
                    None::<Json>.into(),
                ],
                Some(r#"[null, null]"#.parse().unwrap()),
            ),
            (
                vec![
                    Some(Json::from_str(r#"[]"#).unwrap()).into(),
                    Some(b"$".to_vec()).into(),
                    None::<Json>.into(),
                ],
                Some(r#"[null]"#.parse().unwrap()),
            ),
            (
                vec![
                    Some(Json::from_str(r#"{}"#).unwrap()).into(),
                    Some(b"$".to_vec()).into(),
                    None::<Json>.into(),
                ],
                Some(r#"[{}, null]"#.parse().unwrap()),
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a": 1, "b": [2, 3], "c": 4}"#).unwrap()).into(),
                    None::<Bytes>.into(),
                    None::<Json>.into(),
                ],
                None::<Json>,
            ),
            // Following tests come from MySQL doc.
            (
                vec![
                    Some(Json::from_str(r#"["a", ["b", "c"], "d"]"#).unwrap()).into(),
                    Some(b"$[1]".to_vec()).into(),
                    Some(Json::from_u64(1).unwrap()).into(),
                ],
                Some(r#"["a", ["b", "c", 1], "d"]"#.parse().unwrap()),
            ),
            (
                vec![
                    Some(Json::from_str(r#"["a", ["b", "c"], "d"]"#).unwrap()).into(),
                    Some(b"$[0]".to_vec()).into(),
                    Some(Json::from_u64(2).unwrap()).into(),
                ],
                Some(r#"[["a", 2], ["b", "c"], "d"]"#.parse().unwrap()),
            ),
            (
                vec![
                    Some(Json::from_str(r#"["a", ["b", "c"], "d"]"#).unwrap()).into(),
                    Some(b"$[1][0]".to_vec()).into(),
                    Some(Json::from_u64(3).unwrap()).into(),
                ],
                Some(r#"["a", [["b", 3], "c"], "d"]"#.parse().unwrap()),
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a": 1, "b": [2, 3], "c": 4}"#).unwrap()).into(),
                    Some(b"$.b".to_vec()).into(),
                    Some(Json::from_str(r#""x""#).unwrap()).into(),
                ],
                Some(r#"{"a": 1, "b": [2, 3, "x"], "c": 4}"#.parse().unwrap()),
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a": 1, "b": [2, 3], "c": 4}"#).unwrap()).into(),
                    Some(b"$.c".to_vec()).into(),
                    Some(Json::from_str(r#""y""#).unwrap()).into(),
                ],
                Some(r#"{"a": 1, "b": [2, 3], "c": [4, "y"]}"#.parse().unwrap()),
            ),
            // Following tests come from MySQL test.
            (
                vec![
                    Some(Json::from_str(r#"[1,2,3, {"a":[4,5,6]}]"#).unwrap()).into(),
                    Some(b"$".to_vec()).into(),
                    Some(Json::from_u64(7).unwrap()).into(),
                ],
                Some(r#"[1, 2, 3, {"a": [4, 5, 6]}, 7]"#.parse().unwrap()),
            ),
            (
                vec![
                    Some(Json::from_str(r#"[1,2,3, {"a":[4,5,6]}]"#).unwrap()).into(),
                    Some(b"$".to_vec()).into(),
                    Some(Json::from_u64(7).unwrap()).into(),
                    Some(b"$[3].a".to_vec()).into(),
                    Some(Json::from_f64(3.15).unwrap()).into(),
                ],
                Some(r#"[1, 2, 3, {"a": [4, 5, 6, 3.15]}, 7]"#.parse().unwrap()),
            ),
            (
                vec![
                    Some(Json::from_str(r#"[1,2,3, {"a":[4,5,6]}]"#).unwrap()).into(),
                    Some(b"$".to_vec()).into(),
                    Some(Json::from_u64(7).unwrap()).into(),
                    Some(b"$[3].b".to_vec()).into(),
                    Some(Json::from_u64(8).unwrap()).into(),
                ],
                Some(r#"[1, 2, 3, {"a": [4, 5, 6]}, 7]"#.parse().unwrap()),
            ),
        ];
        for (args, expect_output) in cases {
            let output: Option<Json> = RpnFnScalarEvaluator::new()
                .push_params(args.clone())
                .evaluate(ScalarFuncSig::JsonArrayAppendSig)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}", args);
        }
    }

    #[test]
    fn test_json_merge_patch() {
        let cases = vec![
            // RFC 7396 document: https://datatracker.ietf.org/doc/html/rfc7396
            // RFC 7396 Example Test Cases
            (
                vec![Some(r#"{"a":"b"}"#), Some(r#"{"a":"c"}"#)],
                Some(r#"{"a": "c"}"#),
            ),
            (
                vec![Some(r#"{"a":"b"}"#), Some(r#"{"b":"c"}"#)],
                Some(r#"{"a": "b","b": "c"}"#),
            ),
            (
                vec![Some(r#"{"a":"b"}"#), Some(r#"{"a":null}"#)],
                Some(r#"{}"#),
            ),
            (
                vec![Some(r#"{"a":"b", "b":"c"}"#), Some(r#"{"a":null}"#)],
                Some(r#"{"b": "c"}"#),
            ),
            (
                vec![Some(r#"{"a":["b"]}"#), Some(r#"{"a":"c"}"#)],
                Some(r#"{"a": "c"}"#),
            ),
            (
                vec![Some(r#"{"a":"c"}"#), Some(r#"{"a":["b"]}"#)],
                Some(r#"{"a": ["b"]}"#),
            ),
            (
                vec![
                    Some(r#"{"a":{"b":"c"}}"#),
                    Some(r#"{"a":{"b":"d","c":null}}"#),
                ],
                Some(r#"{"a": {"b": "d"}}"#),
            ),
            (
                vec![Some(r#"{"a":[{"b":"c"}]}"#), Some(r#"{"a": [1]}"#)],
                Some(r#"{"a": [1]}"#),
            ),
            (
                vec![Some(r#"["a","b"]"#), Some(r#"["c","d"]"#)],
                Some(r#"["c", "d"]"#),
            ),
            (
                vec![Some(r#"{"a":"b"}"#), Some(r#"["c"]"#)],
                Some(r#"["c"]"#),
            ),
            (
                vec![Some(r#"{"a":"foo"}"#), Some(r#"null"#)],
                Some(r#"null"#),
            ),
            (
                vec![Some(r#"{"a":"foo"}"#), Some(r#""bar""#)],
                Some(r#""bar""#),
            ),
            (
                vec![Some(r#"{"e":null}"#), Some(r#"{"a":1}"#)],
                Some(r#"{"e": null,"a": 1}"#),
            ),
            (
                vec![Some(r#"[1,2]"#), Some(r#"{"a":"b","c":null}"#)],
                Some(r#"{"a":"b"}"#),
            ),
            (
                vec![Some(r#"{}"#), Some(r#"{"a":{"bb":{"ccc":null}}}"#)],
                Some(r#"{"a":{"bb": {}}}"#),
            ),
            // RFC 7396 Example Document
            (
                vec![
                    Some(
                        r#"{"title":"Goodbye!","author":{"givenName":"John","familyName":"Doe"},"tags":["example","sample"],"content":"This will be unchanged"}"#,
                    ),
                    Some(
                        r#"{"title":"Hello!","phoneNumber":"+01-123-456-7890","author":{"familyName":null},"tags":["example"]}"#,
                    ),
                ],
                Some(
                    r#"{"title":"Hello!","author":{"givenName":"John"},"tags":["example"],"content":"This will be unchanged","phoneNumber":"+01-123-456-7890"}"#,
                ),
            ),
            // From mysql Example Test Cases
            (
                vec![
                    None,
                    Some(r#"null"#),
                    Some(r#"[1,2,3]"#),
                    Some(r#"{"a":1}"#),
                ],
                Some(r#"{"a": 1}"#),
            ),
            (
                vec![
                    Some(r#"null"#),
                    None,
                    Some(r#"[1,2,3]"#),
                    Some(r#"{"a":1}"#),
                ],
                Some(r#"{"a": 1}"#),
            ),
            (
                vec![
                    Some(r#"null"#),
                    Some(r#"[1,2,3]"#),
                    None,
                    Some(r#"{"a":1}"#),
                ],
                None,
            ),
            (
                vec![
                    Some(r#"null"#),
                    Some(r#"[1,2,3]"#),
                    Some(r#"{"a":1}"#),
                    None,
                ],
                None,
            ),
            (
                vec![
                    None,
                    Some(r#"null"#),
                    Some(r#"{"a":1}"#),
                    Some(r#"[1,2,3]"#),
                ],
                Some(r#"[1,2,3]"#),
            ),
            (
                vec![
                    Some(r#"null"#),
                    None,
                    Some(r#"{"a":1}"#),
                    Some(r#"[1,2,3]"#),
                ],
                Some(r#"[1,2,3]"#),
            ),
            (
                vec![
                    Some(r#"null"#),
                    Some(r#"{"a":1}"#),
                    None,
                    Some(r#"[1,2,3]"#),
                ],
                Some(r#"[1,2,3]"#),
            ),
            (
                vec![
                    Some(r#"null"#),
                    Some(r#"{"a":1}"#),
                    Some(r#"[1,2,3]"#),
                    None,
                ],
                None,
            ),
            (
                vec![None, Some(r#"null"#), Some(r#"{"a":1}"#), Some(r#"true"#)],
                Some(r#"true"#),
            ),
            (
                vec![Some(r#"null"#), None, Some(r#"{"a":1}"#), Some(r#"true"#)],
                Some(r#"true"#),
            ),
            (
                vec![Some(r#"null"#), Some(r#"{"a":1}"#), None, Some(r#"true"#)],
                Some(r#"true"#),
            ),
            (
                vec![Some(r#"null"#), Some(r#"{"a":1}"#), Some(r#"true"#), None],
                None,
            ),
            // non-object last item
            (
                vec![
                    Some("true"),
                    Some("false"),
                    Some("[]"),
                    Some("{}"),
                    Some("null"),
                ],
                Some("null"),
            ),
            (
                vec![
                    Some("false"),
                    Some("[]"),
                    Some("{}"),
                    Some("null"),
                    Some("true"),
                ],
                Some("true"),
            ),
            (
                vec![
                    Some("true"),
                    Some("[]"),
                    Some("{}"),
                    Some("null"),
                    Some("false"),
                ],
                Some("false"),
            ),
            (
                vec![
                    Some("true"),
                    Some("false"),
                    Some("{}"),
                    Some("null"),
                    Some("[]"),
                ],
                Some("[]"),
            ),
            (
                vec![
                    Some("true"),
                    Some("false"),
                    Some("{}"),
                    Some("null"),
                    Some("1"),
                ],
                Some("1"),
            ),
            (
                vec![
                    Some("true"),
                    Some("false"),
                    Some("{}"),
                    Some("null"),
                    Some("1.8"),
                ],
                Some("1.8"),
            ),
            (
                vec![
                    Some("true"),
                    Some("false"),
                    Some("{}"),
                    Some("null"),
                    Some("112"),
                ],
                Some("112"),
            ),
            (vec![Some(r#"{"a":"foo"}"#), None], None),
            (vec![None, Some(r#"{"a":"foo"}"#)], None),
            (
                vec![Some(r#"{"a":"foo"}"#), Some(r#"false"#)],
                Some(r#"false"#),
            ),
            (vec![Some(r#"{"a":"foo"}"#), Some(r#"123"#)], Some(r#"123"#)),
            (
                vec![Some(r#"{"a":"foo"}"#), Some(r#"123.1"#)],
                Some(r#"123.1"#),
            ),
            (
                vec![Some(r#"{"a":"foo"}"#), Some(r#"[1,2,3]"#)],
                Some(r#"[1,2,3]"#),
            ),
            (
                vec![Some(r#"null"#), Some(r#"{"a":1}"#)],
                Some(r#"{"a":1}"#),
            ),
            (vec![Some(r#"{"a":1}"#), Some(r#"null"#)], Some(r#"null"#)),
            (
                vec![
                    Some(r#"{"a":"foo"}"#),
                    Some(r#"{"a":null}"#),
                    Some(r#"{"b":"123"}"#),
                    Some(r#"{"c":1}"#),
                ],
                Some(r#"{"b":"123","c":1}"#),
            ),
            (
                vec![
                    Some(r#"{"a":"foo"}"#),
                    Some(r#"{"a":null}"#),
                    Some(r#"{"c":1}"#),
                ],
                Some(r#"{"c":1}"#),
            ),
            (
                vec![
                    Some(r#"{"a":"foo"}"#),
                    Some(r#"{"a":null}"#),
                    Some(r#"true"#),
                ],
                Some(r#"true"#),
            ),
            (
                vec![
                    Some(r#"{"a":"foo"}"#),
                    Some(r#"{"d":1}"#),
                    Some(r#"{"a":{"bb":{"ccc":null}}}"#),
                ],
                Some(r#"{"a":{"bb":{}},"d":1}"#),
            ),
        ];

        for (vargs, expected) in cases {
            let vargs: Vec<Option<Json>> = vargs
                .into_iter()
                .map(|input| input.map(|s| Json::from_str(s).unwrap()))
                .collect::<Vec<_>>();
            let expected = expected.map(|s| Json::from_str(s).unwrap());

            let output = RpnFnScalarEvaluator::new()
                .push_params(vargs.clone())
                .evaluate(ScalarFuncSig::JsonMergePatchSig)
                .unwrap();
            assert_eq!(output, expected, "{:?}", vargs);
        }
    }
}
