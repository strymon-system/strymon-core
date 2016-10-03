pub struct Continuation<T> {
    f: Box<FnOnce(T)>,
}
