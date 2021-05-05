#![deny(missing_docs)]

use std::{
    error::Error,
    fmt::{self, Display},
};
use tracing_error::SpanTrace;

use crate::io_shell;

/// The result type.
pub type DescriberResult<T> = Result<T, DescriberError>;

/// Description errors.
#[derive(Debug)]
pub struct DescriberError {
    kind: DescriberErrorKind,
    context: SpanTrace,
}

impl DescriberError {
    /// The `DescriberErrorKind` wrapped by the error.
    pub fn into_kind(self) -> DescriberErrorKind {
        self.kind
    }

    /// The `DescriberErrorKind` wrapped by the error.
    pub fn kind(&self) -> &DescriberErrorKind {
        &self.kind
    }

    /// The `tracing_error::SpanTrace` contained in the error.
    pub fn span_trace(&self) -> SpanTrace {
        self.context.clone()
    }
}

impl From<DescriberErrorKind> for DescriberError {
    fn from(kind: DescriberErrorKind) -> Self {
        Self {
            kind,
            context: SpanTrace::capture(),
        }
    }
}

/// Variants of DescriberError.
#[derive(Debug)]
pub enum DescriberErrorKind {
    /// IoShellError
    IoShellError(io_shell::DatabaseError),
    #[cfg(feature = "quaint")]
    /// An error originating from Quaint or the database.
    QuaintError(quaint::error::Error),
    /// An illegal cross-schema reference.
    CrossSchemaReference {
        /// Qualified path of the source table.
        from: String,
        /// Qualified path of the referenced table.
        to: String,
        /// Name of the constraint.
        constraint: String,
    },
}

impl Display for DescriberError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind() {
            #[cfg(feature = "quaint")]
            DescriberErrorKind::QuaintError(_) => {
                self.kind().fmt(f)?;
                self.context.fmt(f)
            }
            _ => self.kind().fmt(f),
        }
    }
}

impl Display for DescriberErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IoShellError(_) => todo!(),
            #[cfg(feature = "quaint")]
            Self::QuaintError(err) => err.fmt(f),
            Self::CrossSchemaReference { from, to, constraint } => {
                write!(
                    f,
                    "Illegal cross schema reference from `{}` to `{}` in constraint `{}`. Foreign keys between database schemas are not supported in Prisma. Please follow the GitHub ticket: https://github.com/prisma/prisma/issues/1175",
                    from, to, constraint
                )
            }
        }
    }
}

impl Error for DescriberError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            #[cfg(feature = "quaint")]
            DescriberErrorKind::QuaintError(err) => Some(err),
            DescriberErrorKind::CrossSchemaReference { .. } => None,
            DescriberErrorKind::IoShellError(_) => todo!(),
        }
    }
}

#[cfg(feature = "quaint")]
impl From<quaint::error::Error> for DescriberError {
    fn from(err: quaint::error::Error) -> Self {
        DescriberError {
            kind: DescriberErrorKind::QuaintError(err),
            context: SpanTrace::capture(),
        }
    }
}

impl From<io_shell::DatabaseError> for DescriberError {
    fn from(err: io_shell::DatabaseError) -> Self {
        DescriberError {
            kind: DescriberErrorKind::IoShellError(err),
            context: SpanTrace::capture(),
        }
    }
}
