//! Newest-value handoff from a worker thread to the renderer loop.
//!
//! Publishing also signals a descriptor, so the loop can block on the hardware
//! socket and every producer at once instead of polling channels on a timer.

use std::cell::Cell;
use std::io::{self, Read, Write};
use std::os::fd::{AsFd, BorrowedFd};
use std::os::unix::net::UnixStream;
use std::sync::mpsc::{self, Receiver, Sender};

/// Creates one connected publisher and receiver.
///
/// # Errors
///
/// Returns the socket error when the wake descriptor pair cannot be created.
pub fn channel<T>() -> io::Result<(Publisher<T>, Latest<T>)> {
    let (wake_reader, wake_writer) = UnixStream::pair()?;
    wake_reader.set_nonblocking(true)?;
    wake_writer.set_nonblocking(true)?;
    let (values, updates) = mpsc::channel();
    Ok((
        Publisher {
            values,
            wake: wake_writer,
        },
        Latest {
            updates,
            wake: wake_reader,
            closed: Cell::new(false),
        },
    ))
}

/// Worker-side end. Sending never blocks.
pub struct Publisher<T> {
    values: Sender<T>,
    wake: UnixStream,
}

/// The receiving end is gone, so nothing will read further values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Disconnected;

impl<T> Publisher<T> {
    /// Queues `value` and wakes the receiver.
    ///
    /// # Errors
    ///
    /// Returns [`Disconnected`] once the receiver has been dropped.
    pub fn publish(&self, value: T) -> Result<(), Disconnected> {
        self.values.send(value).map_err(|_| Disconnected)?;
        // A full wake buffer already guarantees a pending wakeup.
        let _ = (&self.wake).write(&[1]);
        Ok(())
    }
}

/// Loop-side end.
pub struct Latest<T> {
    updates: Receiver<T>,
    wake: UnixStream,
    closed: Cell<bool>,
}

impl<T> Latest<T> {
    /// Returns only the newest queued value and discards superseded ones.
    #[must_use]
    pub fn take_latest(&self) -> Option<T> {
        // Clear wakeups before reading values: a value published after this
        // point leaves a fresh wakeup for the next wait.
        let mut drained = [0_u8; 64];
        loop {
            match (&self.wake).read(&mut drained) {
                Ok(0) => {
                    self.closed.set(true);
                    break;
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
        let mut latest = None;
        while let Ok(update) = self.updates.try_recv() {
            latest = Some(update);
        }
        latest
    }

    /// Descriptor that becomes readable when a value has been published, or
    /// `None` once the publisher is gone and its hangup has been observed, so
    /// a dead worker cannot keep the loop awake.
    #[must_use]
    pub fn wake_descriptor(&self) -> Option<BorrowedFd<'_>> {
        (!self.closed.get()).then(|| self.wake.as_fd())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use t1_platform::seqpacket::wait_readable;

    use super::*;

    #[test]
    fn publishing_wakes_the_receiver_and_only_the_newest_value_is_kept() {
        let (publisher, latest) = channel().expect("create channel");
        let idle = Some(Duration::from_millis(1));
        assert_eq!(wait_readable(&[latest.wake_descriptor()], idle), Ok(false));

        let worker = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            publisher.publish(1).expect("publish first");
            publisher.publish(2).expect("publish second");
            publisher
        });
        assert_eq!(wait_readable(&[latest.wake_descriptor()], None), Ok(true));
        let publisher = worker.join().expect("worker thread");
        assert_eq!(latest.take_latest(), Some(2));
        assert_eq!(wait_readable(&[latest.wake_descriptor()], idle), Ok(false));
        assert_eq!(latest.take_latest(), None);

        drop(latest);
        assert_eq!(publisher.publish(3), Err(Disconnected));
    }

    #[test]
    fn a_vanished_publisher_stops_waking_the_receiver() {
        let (publisher, latest) = channel::<u8>().expect("create channel");
        publisher.publish(7).expect("publish");
        drop(publisher);
        assert_eq!(latest.take_latest(), Some(7));
        assert!(latest.wake_descriptor().is_none());
    }
}
