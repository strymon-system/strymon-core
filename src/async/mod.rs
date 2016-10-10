pub mod queue;
pub mod select;
pub mod spawn;

#[cfg(test)]
mod tests {
    use super::queue::*;
    use super::select::*;
    use futures;
    use futures::stream;

    #[test]
    fn select_stream() {
        let mut select = Select::<i32>::new();
        let stream = stream::iter(vec![Ok(17), Err(()), Ok(42)]);
        select.drain(stream);

        assert_eq!(Ok(Some(17)), select.recv());
        assert_eq!(Ok(None), select.recv());
        assert_eq!(Ok(Some(42)), select.recv());
        assert_eq!(Ok(None), select.recv());
        assert_eq!(Err(Empty), select.recv());
    }

    #[test]
    fn select_future() {
        let mut select = Select::<i32>::new();
        assert_eq!(Err(Empty), select.recv());
        select.ensure(futures::lazy(|| Ok(()) ));
        assert_eq!(Ok(None), select.recv());
    }
    
    #[test]
    fn select_mpsc() {
        let mut select = Select::<i32>::new();
        let (tx1, rx1) = channel::<i32, ()>();
        let (tx2, rx2) = channel::<i32, ()>();

        select.drain(rx1);
        select.drain(rx2);

        tx2.send(Ok(2)).unwrap();
        assert_eq!(Ok(Some(2)), select.recv());
        tx1.send(Ok(1)).unwrap();
        assert_eq!(Ok(Some(1)), select.recv());

        drop(tx1);
        assert_eq!(Ok(None), select.recv());
    }
}
