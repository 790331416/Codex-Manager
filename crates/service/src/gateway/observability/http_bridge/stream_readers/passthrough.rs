use super::{
    classify_upstream_stream_read_error, inspect_sse_frame, merge_usage, sse_keepalive_interval,
    stream_reader_disconnected_message, upstream_hint_or_stream_incomplete_message, Arc, Cursor,
    Mutex, PassthroughSseCollector, Read, SseKeepAliveFrame, SseTerminal, UpstreamSseFramePump,
    UpstreamSseFramePumpItem,
};
use crate::gateway::http_bridge::extract_error_hint_from_body;

pub(crate) struct PassthroughSseUsageReader {
    upstream: UpstreamSseFramePump,
    out_cursor: Cursor<Vec<u8>>,
    usage_collector: Arc<Mutex<PassthroughSseCollector>>,
    keepalive_frame: SseKeepAliveFrame,
    finished: bool,
}

impl PassthroughSseUsageReader {
    /// 函数 `new`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - crate: 参数 crate
    ///
    /// # 返回
    /// 返回函数执行结果
    pub(crate) fn new(
        upstream: reqwest::blocking::Response,
        usage_collector: Arc<Mutex<PassthroughSseCollector>>,
        keepalive_frame: SseKeepAliveFrame,
    ) -> Self {
        Self {
            upstream: UpstreamSseFramePump::new(upstream),
            out_cursor: Cursor::new(Vec::new()),
            usage_collector,
            keepalive_frame,
            finished: false,
        }
    }

    /// 函数 `update_usage_from_frame`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - self: 参数 self
    /// - lines: 参数 lines
    ///
    /// # 返回
    /// 无
    fn update_usage_from_frame(&self, lines: &[String]) {
        let inspection = inspect_sse_frame(lines);
        if let Ok(mut collector) = self.usage_collector.lock() {
            if let Some(event_type) = inspection.last_event_type {
                collector.last_event_type = Some(event_type);
            }
            if inspection.usage.is_none() && inspection.terminal.is_none() {
                if collector.upstream_error_hint.is_none() {
                    let raw_frame = lines.concat();
                    let trimmed = raw_frame.trim();
                    let looks_like_sse_frame = lines.iter().any(|line| {
                        let line = line.trim_start();
                        line.starts_with("data:")
                            || line.starts_with("event:")
                            || line.starts_with("id:")
                            || line.starts_with("retry:")
                            || line.starts_with(':')
                    });
                    if !looks_like_sse_frame && !trimmed.is_empty() {
                        collector.upstream_error_hint =
                            extract_error_hint_from_body(400, raw_frame.as_bytes())
                                .or_else(|| Some(trimmed.to_string()));
                    }
                }
                return;
            }
            if let Some(parsed) = inspection.usage {
                merge_usage(&mut collector.usage, parsed);
            }
            if let Some(terminal) = inspection.terminal {
                collector.saw_terminal = true;
                if let SseTerminal::Err(message) = terminal {
                    collector.terminal_error = Some(message);
                }
            }
        }
    }

    /// 函数 `next_chunk`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - self: 参数 self
    ///
    /// # 返回
    /// 返回函数执行结果
    fn next_chunk(&mut self) -> std::io::Result<Vec<u8>> {
        match self.upstream.recv_timeout(sse_keepalive_interval()) {
            Ok(UpstreamSseFramePumpItem::Frame(frame)) => {
                self.update_usage_from_frame(&frame);
                Ok(frame.concat().into_bytes())
            }
            Ok(UpstreamSseFramePumpItem::Eof) => {
                if let Ok(mut collector) = self.usage_collector.lock() {
                    if !collector.saw_terminal {
                        let hint = collector.upstream_error_hint.clone();
                        collector.terminal_error.get_or_insert_with(|| {
                            upstream_hint_or_stream_incomplete_message(hint.as_deref())
                        });
                    }
                }
                self.finished = true;
                Ok(Vec::new())
            }
            Ok(UpstreamSseFramePumpItem::Error(err)) => {
                if let Ok(mut collector) = self.usage_collector.lock() {
                    collector
                        .terminal_error
                        .get_or_insert_with(|| classify_upstream_stream_read_error(&err));
                }
                self.finished = true;
                Ok(Vec::new())
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                Ok(self.keepalive_frame.bytes().to_vec())
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                if let Ok(mut collector) = self.usage_collector.lock() {
                    let hint = collector.upstream_error_hint.clone();
                    collector.terminal_error.get_or_insert_with(|| {
                        hint.unwrap_or_else(stream_reader_disconnected_message)
                    });
                }
                self.finished = true;
                Ok(Vec::new())
            }
        }
    }
}

impl Read for PassthroughSseUsageReader {
    /// 函数 `read`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - self: 参数 self
    /// - buf: 参数 buf
    ///
    /// # 返回
    /// 返回函数执行结果
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            let read = self.out_cursor.read(buf)?;
            if read > 0 {
                return Ok(read);
            }
            if self.finished {
                return Ok(0);
            }
            self.out_cursor = Cursor::new(self.next_chunk()?);
        }
    }
}
