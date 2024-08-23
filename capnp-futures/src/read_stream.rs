// Copyright (c) 2016 Sandstorm Development Group, Inc. and contributors
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

use capnp::serialize::OwnedSegments;
use futures::future::Future;
use futures::stream::Stream;
use futures::{pin_mut, AsyncRead};
use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};

use capnp::{message, Error};

pin_project! {
    /// An incoming sequence of messages.
    #[must_use = "streams do nothing unless polled"]
    pub struct ReadStream<R: AsyncRead>
    {
        options: message::ReaderOptions,
        #[pin]
        reader: R,
    }
}

impl<R: AsyncRead> ReadStream<R> {
    pub fn new(reader: R, options: message::ReaderOptions) -> Self {
        ReadStream { reader, options }
    }
}

impl<R: AsyncRead> Stream for ReadStream<R> {
    type Item = Result<message::Reader<OwnedSegments>, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let next_message_fut =
            crate::serialize::try_read_message(this.reader, this.options.clone());

        pin_mut!(next_message_fut);
        match Future::poll(next_message_fut, cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
            Poll::Ready(Ok(None)) => Poll::Ready(None),
            Poll::Ready(Ok(Some(message))) => Poll::Ready(Some(Ok(message))),
        }
    }
}
