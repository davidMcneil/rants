#[cfg(test)]
mod tests;

use bytes::{BufMut, BytesMut};
use log::trace;
use std::{io, mem, str, usize};
use tokio::codec::Decoder;

use crate::{
    types::{Msg, RantsError, RantsResult, ServerControl, ServerMessage, Subject},
    util::MESSAGE_TERMINATOR,
};

enum State {
    /// We are currently reading a control line and trying to parse it.
    ReadControl,
    /// We are currently reading the payload of a `Msg` message.
    ReadMsgPayload {
        subject: Subject,
        sid: String,
        reply_to: Option<Subject>,
        len: usize,
    },
}

/// A `ServerMessage` codec.
pub struct Codec {
    // Stored index of the next index to examine for a `\n` character when reading a control line.
    // This is used to optimize searching.
    next_index: usize,
    /// The current state.
    state: State,
}

impl Codec {
    /// Returns a `Codec` for parsing out `ServerMessage`s.
    pub fn new() -> Codec {
        Codec {
            next_index: 0,
            state: State::ReadControl,
        }
    }

    fn decode_impl(&mut self, buf: &mut BytesMut) -> RantsResult<ServerMessage> {
        match &mut self.state {
            State::ReadMsgPayload { len, .. } => {
                let len = *len;
                // Check if the payload is complete
                if buf.len() < len + MESSAGE_TERMINATOR.len() {
                    return Err(RantsError::NotEnoughData);
                }
                let line = buf.split_to(len + MESSAGE_TERMINATOR.len());
                let terminator = &line[len..len + MESSAGE_TERMINATOR.len()];
                let payload = &line[..len];
                // Check that the payload is correctly terminated
                if terminator != MESSAGE_TERMINATOR.as_bytes() {
                    // We are in an invalid state. Try and recover by reading a control line.
                    self.state = State::ReadControl;
                    return Err(RantsError::InvalidTerminator(terminator.to_vec()));
                }
                // This is messy, but it is pretty straightforward. We set `self.state` to
                // `ReadControl` and then construct a `Msg` from the components of
                // `old_state`. We know that `old_state` must be the `ReadMsgPayload`
                // state because that is what type `self.state` was.
                let old_state = mem::replace(&mut self.state, State::ReadControl);
                if let State::ReadMsgPayload {
                    subject,
                    sid,
                    reply_to,
                    ..
                } = old_state
                {
                    // We should always make it here
                    return Ok(ServerMessage::Msg(Msg::new(
                        subject,
                        sid,
                        reply_to,
                        payload.to_vec(),
                    )));
                }
                unreachable!();
            }
            State::ReadControl => {
                let newline_offset = buf[self.next_index..].iter().position(|b| *b == b'\n');
                if let Some(offset) = newline_offset {
                    // Found a control line
                    let newline_index = offset + self.next_index;
                    self.next_index = 0;
                    let line = buf.split_to(newline_index + 1);
                    let line = utf8(&line)?;
                    // Parse the control line
                    let control_line = line.parse()?;
                    trace!("<<- {:?}", line);
                    if let ServerControl::Msg {
                        subject,
                        sid,
                        reply_to,
                        len,
                    } = control_line
                    {
                        // If the message is a `Msg` enter the `ReadMsgPayload` state
                        let len = len as usize;
                        self.state = State::ReadMsgPayload {
                            subject,
                            sid,
                            reply_to,
                            len,
                        };
                        // Reserve space in the buffer for the payload and call decode
                        // again this time to read the payload
                        buf.reserve(len + MESSAGE_TERMINATOR.len());
                        self.decode_impl(buf)
                    } else {
                        // Convert the control line to an actual message
                        Ok(control_line.into())
                    }
                } else {
                    // We didn't find a line, so the next call will resume searching at the current
                    // end of the buffer.
                    self.next_index = buf.len();
                    Err(RantsError::NotEnoughData)
                }
            }
        }
    }
}

fn utf8(buf: &[u8]) -> Result<&str, io::Error> {
    str::from_utf8(buf)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Unable to decode input as UTF8"))
}

impl Decoder for Codec {
    type Error = io::Error;
    type Item = RantsResult<ServerMessage>;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let result = self.decode_impl(buf);
        if let Err(RantsError::NotEnoughData) = result {
            return Ok(None);
        }
        Ok(Some(result))
    }
}
