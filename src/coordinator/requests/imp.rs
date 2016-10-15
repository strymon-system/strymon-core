use abomonation::Abomonation;

use super::*;

impl Abomonation for Placement {
    #[inline]
    unsafe fn embalm(&mut self) {
        match *self {
            Placement::Random(_, _) => (),
            Placement::Fixed(ref mut e, _) => e.embalm(),
        }
    }
    #[inline]
    unsafe fn entomb(&self, bytes: &mut Vec<u8>) {
        match *self {
            Placement::Random(_, _) => (),
            Placement::Fixed(ref e, _) => e.entomb(bytes),
        }
    }
    #[inline]
    unsafe fn exhume<'a, 'b>(&'a mut self, bytes: &'b mut [u8]) -> Option<&'b mut [u8]> {
        match *self {
            Placement::Random(_, _) => Some(bytes),
            Placement::Fixed(ref mut e, _) => e.exhume(bytes),
        }
    }
}

unsafe_abomonate!(Submission: query, name, placement);
unsafe_abomonate!(SubmissionError);
