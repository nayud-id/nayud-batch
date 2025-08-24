use nayud_batch::types::response::{ApiResponse, ApiMessage, CODE_SUCCESS, CODE_FAILURE};
use nayud_batch::errors::AppError;

#[test]
fn response_ok_success_failure() {
    let r1: ApiResponse<i32> = ApiResponse::ok("msg", Some(7));
    assert_eq!(r1.code, CODE_SUCCESS);
    assert!(r1.is_success());
    match &r1.message {
        ApiMessage::Text(s) => assert_eq!(s, "msg"),
        _ => panic!("expected text message for success"),
    }
    assert_eq!(r1.data, Some(7));

    let r2: ApiResponse<i32> = ApiResponse::success_with("ok", 42);
    assert_eq!(r2.code, CODE_SUCCESS);
    assert!(r2.is_success());
    assert_eq!(r2.data, Some(42));

    let r3: ApiResponse<i32> = ApiResponse::failure("nope");
    assert_eq!(r3.code, CODE_FAILURE);
    assert!(!r3.is_success());
    assert!(r3.data.is_none());
    match &r3.message {
        ApiMessage::Detail { what, why, how } => {
            assert_eq!(what, "nope");
            assert!(!why.is_empty());
            assert!(!how.is_empty());
        }
        _ => panic!("expected detail message for failure"),
    }
}

#[test]
fn response_from_result_and_option_and_error() {
    let r_ok: ApiResponse<i32> = ApiResponse::from_result(Ok(5), "yay");
    assert_eq!(r_ok.code, CODE_SUCCESS);
    assert_eq!(r_ok.data, Some(5));
    match &r_ok.message {
        ApiMessage::Text(s) => assert_eq!(s, "yay"),
        _ => panic!("expected text message for success from_result"),
    }

    let r_err: ApiResponse<i32> = ApiResponse::from_result(Err(AppError::db("boom")), "ignored");
    assert_eq!(r_err.code, CODE_FAILURE);
    assert!(r_err.data.is_none());
    match &r_err.message {
        ApiMessage::Detail { what, why, how } => {
            assert!(what.contains("Database error: boom"));
            assert!(!why.is_empty());
            assert!(!how.is_empty());
        }
        _ => panic!("expected detail message for failure from_result"),
    }

    let r_some: ApiResponse<&str> = ApiResponse::from_option(Some("v"), "has", "none");
    assert_eq!(r_some.code, CODE_SUCCESS);
    assert_eq!(r_some.data, Some("v"));

    let r_none: ApiResponse<&str> = ApiResponse::from_option(None, "has", "none");
    assert_eq!(r_none.code, CODE_FAILURE);
    assert!(r_none.data.is_none());

    let r_empty: ApiResponse<()> = ApiResponse::success("done");
    assert_eq!(r_empty.code, CODE_SUCCESS);

    let e = AppError::other("x");
    let r_from_err = ApiResponse::from_error(&e);
    assert_eq!(r_from_err.code, CODE_FAILURE);
    match &r_from_err.message {
        ApiMessage::Detail { what, why, how } => {
            assert!(what.contains("Unexpected error: x"));
            assert!(!why.is_empty());
            assert!(!how.is_empty());
        }
        _ => panic!("expected detail message for from_error"),
    }
}