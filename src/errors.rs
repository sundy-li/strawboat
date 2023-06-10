macro_rules! general_err {
    ($fmt:expr) => (Error::OutOfSpec($fmt.to_owned()));
    ($fmt:expr, $($args:expr),*) => (Error::OutOfSpec(format!($fmt, $($args),*)));
    ($e:expr, $fmt:expr) => (Error::OutOfSpec($fmt.to_owned(), $e));
    ($e:ident, $fmt:expr, $($args:tt),*) => (
        Error::OutOfSpec(&format!($fmt, $($args),*), $e));
}

macro_rules! nyi_err {
    ($fmt:expr) => (Error::NotYetImplemented($fmt.to_owned()));
    ($fmt:expr, $($args:expr),*) => (Error::NotYetImplemented(format!($fmt, $($args),*)));
}

macro_rules! io_err {
    ($fmt:expr) => (Error::OutOfSpec($fmt.to_owned()));
    ($fmt:expr, $($args:expr),*) => (Error::OutOfSpec(format!($fmt, $($args),*)));
}
