use request_handlers::{GQLError, PrismaResponse};
use serde_json::{Map, Value};

enum QueryResponseType {
    Prisma(PrismaResponse),
    Json(Map<String, Value>),
}

// pub trait QueryResponseTypeInterface {
//     fn failed(&self) -> bool;
//     fn assert_failure(&self, err_code: usize, msg_contains: Option<String>);
// }

pub struct QueryResult {
    response: QueryResponseType,
}

pub struct QueryError {
    code: Option<String>,
    msg: String,
}

impl QueryError {
    fn code(&self) -> Option<&str> {
        self.code.as_deref()
    }

    fn message(&self) -> &str {
        self.msg.as_str()
    }
}

// impl QueryResponseTypeInterface for PrismaResponse {
//     fn failed(&self) -> bool {
//         match self {
//             PrismaResponse::Single(ref s) => s.errors().next().is_some(),
//             PrismaResponse::Multi(ref m) => m.errors().next().is_some(),
//         }
//     }

//     fn assert_failure(&self, err_code: usize, msg_contains: Option<String>) {
//         if !self.failed() {
//             panic!(
//                 "Expected result to return an error, but found success: {}",
//                 self.to_string()
//             );
//         }

//         // 0 is the "do nothing marker"
//         if err_code == 0 {
//             return;
//         }

//         let err_code = format!("P{}", err_code);
//         let err_exists = self.errors().into_iter().any(|err| {
//             let code_matches = err.code() == Some(&err_code);
//             let msg_matches = match msg_contains.as_ref() {
//                 Some(msg) => err.message().contains(msg),
//                 None => true,
//             };

//             code_matches && msg_matches
//         });

//         if !err_exists {
//             if let Some(msg) = msg_contains {
//                 panic!(
//                     "Expected error with code `{}` and message `{}`, got: `{}`",
//                     err_code,
//                     msg,
//                     self.to_string()
//                 );
//             } else {
//                 panic!("Expected error with code `{}`, got: `{}`", err_code, self.to_string());
//             }
//         }
//     }
// }

impl QueryResult {
    pub fn failed(&self) -> bool {
        // self.failed()
        self.errors().len() > 0
    }

    /// Asserts absence of errors in the result. Panics with assertion error.
    pub fn assert_success(&self) {
        assert!(!self.failed())
    }

    /// Asserts presence of errors in the result.
    /// Code must equal the given one, the message is a partial match.
    /// If more than one error is contained, asserts that at least one error contains the message _and_ code.
    ///
    /// Panics with assertion error on no match.
    pub fn assert_failure(&self, err_code: usize, msg_contains: Option<String>) {
        if !self.failed() {
            panic!(
                "Expected result to return an error, but found success: {}",
                self.to_string()
            );
        }

        // 0 is the "do nothing marker"
        if err_code == 0 {
            return;
        }

        let err_code = format!("P{}", err_code);
        let err_exists = self.errors().into_iter().any(|err| {
            let code_matches = err.code() == Some(&err_code);
            let msg_matches = match msg_contains.as_ref() {
                Some(msg) => err.message().contains(msg),
                None => true,
            };
            println!("CODE {:?} {:?}", err.code(), code_matches);
            println!("msg {:?} {:?}", err.message(), msg_matches);

            code_matches && msg_matches
        });

        if !err_exists {
            if let Some(msg) = msg_contains {
                panic!(
                    "Expected error with code `{}` and message `{}`, got: `{}`",
                    err_code,
                    msg,
                    self.to_string()
                );
            } else {
                panic!("Expected error with code `{}`, got: `{}`", err_code, self.to_string());
            }
        }
    }

    pub fn errors(&self) -> Vec<QueryError> {
        match self.response {
            QueryResponseType::Prisma(ref response) => response.errors().iter().map(QueryError::from).collect(),
            QueryResponseType::Json(ref val) => {
                if val.contains_key("errors") {
                    let json_errors = val.get("errors").unwrap().as_array().unwrap();

                    json_errors
                        .iter()
                        .map(|err_val| {
                            let error = err_val.as_object().unwrap();
                            let code = if let Some(user_facing_error) = error.get("user_facing_error") {
                                match user_facing_error.get("error_code") {
                                    Some(val) => Some(serde_json::from_value(val.clone()).unwrap()),
                                    None => None,
                                }
                            } else {
                                None
                            };

                            QueryError {
                                code,
                                msg: err_val.to_string(),
                            }
                        })
                        .collect()
                } else {
                    Vec::new()
                }
            }
        }
    }
}

impl ToString for QueryResult {
    fn to_string(&self) -> String {
        match self.response {
            QueryResponseType::Json(ref val) => serde_json::to_string(val).unwrap(),
            QueryResponseType::Prisma(ref response) => serde_json::to_string(response).unwrap(),
        }
    }
}

impl From<PrismaResponse> for QueryResult {
    fn from(response: PrismaResponse) -> Self {
        Self {
            response: QueryResponseType::Prisma(response),
        }
    }
}

impl From<&&GQLError> for QueryError {
    fn from(error: &&GQLError) -> QueryError {
        QueryError {
            code: error.code().map(String::from),
            msg: error.message().to_string(),
        }
    }
}

impl From<serde_json::Value> for QueryResult {
    fn from(response: serde_json::Value) -> Self {
        let obj = response.as_object().unwrap();
        Self {
            response: QueryResponseType::Json(obj.clone()),
        }
    }
}
