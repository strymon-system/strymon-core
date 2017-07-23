use abomonation::Abomonation;

use super::*;

unsafe_abomonate!(Submission: query, name, placement);

unsafe_abomonate!(AddExecutor: host, ports, format);
unsafe_abomonate!(ExecutorError);

unsafe_abomonate!(AddWorkerGroup: query, group);
unsafe_abomonate!(QueryToken);
unsafe_abomonate!(WorkerGroupError);

unsafe_abomonate!(Subscribe: name, blocking, token);
unsafe_abomonate!(SubscribeError);

unsafe_abomonate!(Unsubscribe: topic, token);
unsafe_abomonate!(UnsubscribeError);

unsafe_abomonate!(Publish: name, addr, schema, token);
unsafe_abomonate!(PublishError);

unsafe_abomonate!(Unpublish: topic, token);
unsafe_abomonate!(UnpublishError);

unsafe_abomonate!(Lookup: name);

unsafe_abomonate!(AddKeeperWorker: name, worker_num, addr);
unsafe_abomonate!(AddKeeperWorkerError);

unsafe_abomonate!(RemoveKeeperWorker: name, worker_num);
unsafe_abomonate!(RemoveKeeperWorkerError);

unsafe_abomonate!(GetKeeperAddress: name);
unsafe_abomonate!(GetKeeperAddressError);

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

impl Abomonation for SubmissionError {
    #[inline]
    unsafe fn embalm(&mut self) {
        match *self {
            SubmissionError::ExecutorsNotFound |
            SubmissionError::ExecutorUnreachable => (),
            SubmissionError::SpawnError(ref mut e) => e.embalm(),
        }
    }
    #[inline]
    unsafe fn entomb(&self, bytes: &mut Vec<u8>) {
        match *self {
            SubmissionError::ExecutorsNotFound |
            SubmissionError::ExecutorUnreachable => (),
            SubmissionError::SpawnError(ref e) => e.entomb(bytes),
        }
    }
    #[inline]
    unsafe fn exhume<'a, 'b>(&'a mut self, bytes: &'b mut [u8]) -> Option<&'b mut [u8]> {
        match *self {
            SubmissionError::ExecutorsNotFound |
            SubmissionError::ExecutorUnreachable => Some(bytes),
            SubmissionError::SpawnError(ref mut e) => e.exhume(bytes),
        }
    }
}
