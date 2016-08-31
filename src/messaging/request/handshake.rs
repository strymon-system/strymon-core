use abomonation::Abomonation;

use messaging::request::Request;

#[derive(Debug, Clone)]
pub struct Handshake<R: Request>(R);

#[derive(Debug, Clone)]
pub enum Response<R: Request> {
    Success(R::Success),
    Error(R::Error),
}
    
impl<R: Request> Abomonation for Response<R>
    where R::Success: Abomonation,
          R::Error: Abomonation,
{
    #[inline]
    unsafe fn embalm(&mut self) {
        match *self {
            Response::Success(ref mut t) => t.embalm(),
            Response::Error(ref mut e) => e.embalm(),
        }
    }

    #[inline]
    unsafe fn entomb(&self, bytes: &mut Vec<u8>) {
        match *self {
            Response::Success(ref t) => t.entomb(bytes),
            Response::Error(ref e) => e.entomb(bytes),
        }
    }
    #[inline]
    unsafe fn exhume<'a, 'b>(&'a mut self, bytes: &'b mut [u8]) -> Option<&'b mut [u8]> {
        match *self {
            Response::Success(ref mut t) => t.exhume(bytes),
            Response::Error(ref mut e) => e.exhume(bytes),
        }
    }
}
