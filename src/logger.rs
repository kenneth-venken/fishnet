use std::{
    cmp::{max, min},
    fmt, io,
    io::{IsTerminal as _, Write as _},
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

use shakmaty::variant::Variant;
use url::Url;

use crate::{
    api::{BatchId, PositionIndex},
    configure::Verbose,
    ipc::{Chunk, Position, PositionResponse},
    util::NevermindExt as _,
};

#[derive(Clone)]
pub struct Logger {
    verbose: Verbose,
    stderr: bool,
    terminal: bool,
    state: Arc<Mutex<LoggerState>>,
}

impl Logger {
    pub fn new(verbose: Verbose, stderr: bool) -> Logger {
        Logger {
            verbose,
            stderr,
            terminal: io::stdout().is_terminal(),
            state: Arc::new(Mutex::new(LoggerState {
                progress_line: 0,
                last_fishnet_line: None,
                fishnet_mid_line: false,
            })),
        }
    }

    fn println(&self, line: &str) {
        let mut state = self.state.lock().expect("logger state");
        state.end_fishnet_dot_run_if_needed(self.stderr);
        state.line_feed();

        if self.stderr {
            writeln!(io::stderr(), "{line}").nevermind("log to stderr");
        } else if let Err(e) = writeln!(io::stdout(), "{line}") {
            // Error when printing to stdout - print error and original
            // line to stderr.
            writeln!(io::stderr(), "E: {e} while logging to stdout: {line}")
                .nevermind("log to stderr");
        }
    }

    pub fn clear_echo(&self) {
        let mut state = self.state.lock().expect("logger state");
        state.end_fishnet_dot_run_if_needed(self.stderr);
        state.line_feed();
    }

    pub fn headline(&self, title: &str) {
        self.println(&format!("\n### {title}\n"));
    }

    pub fn debug(&self, line: &str) {
        if self.verbose.level > 0 {
            self.println(&format!("D: {line}"));
        }
    }

    pub fn info(&self, line: &str) {
        self.println(line);
    }

    /// Repeated identical summaries print as `.` on one line (flush each time).
    pub fn fishnet_info(&self, line: &str) {
        let mut state = self.state.lock().expect("logger state");
        let is_repeat = state
            .last_fishnet_line
            .as_deref()
            .is_some_and(|prev| prev == line);
        if is_repeat {
            if self.stderr {
                write!(io::stderr(), ".").nevermind("log");
                io::stderr().flush().nevermind("flush");
            } else {
                write!(io::stdout(), ".").nevermind("log");
                io::stdout().flush().nevermind("flush");
            }
            state.fishnet_mid_line = true;
            return;
        }

        state.end_fishnet_dot_run_if_needed(self.stderr);
        state.line_feed();
        let out = format!("><> {line}");
        if self.stderr {
            writeln!(io::stderr(), "{out}").nevermind("log to stderr");
        } else if let Err(e) = writeln!(io::stdout(), "{out}") {
            writeln!(io::stderr(), "E: {e} while logging to stdout: {out}")
                .nevermind("log to stderr");
        }
        state.last_fishnet_line = Some(line.to_owned());
        state.fishnet_mid_line = false;
    }

    pub fn warn(&self, line: &str) {
        self.println(&format!("W: {line}"));
    }

    pub fn error(&self, line: &str) {
        self.println(&format!("E: {line}"));
    }

    pub fn progress<P>(&self, queue: QueueStatusBar, progress: P)
    where
        P: Into<ProgressAt>,
    {
        let line = format!(
            "{} {} cores, {} queued, latest: {}",
            queue,
            queue.cores,
            queue.pending,
            progress.into()
        );
        if self.terminal {
            let mut state = self.state.lock().expect("logger state");
            state.end_fishnet_dot_run_if_needed(self.stderr);
            print!(
                "\r{}{}",
                line,
                " ".repeat(state.progress_line.saturating_sub(line.len()))
            );
            io::stdout().flush().expect("flush stdout");
            state.progress_line = line.len();
        } else if self.verbose.level > 0 {
            self.println(&line);
        }
    }
}

pub struct ProgressAt {
    pub batch_id: BatchId,
    pub batch_url: Option<Url>,
    pub position_index: Option<PositionIndex>,
}

impl fmt::Display for ProgressAt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref batch_url) = self.batch_url {
            let mut url = batch_url.clone();
            if let Some(PositionIndex(positon_id)) = self.position_index {
                url.set_fragment(Some(&positon_id.to_string()));
            }
            fmt::Display::fmt(&url, f)
        } else {
            write!(f, "{}", self.batch_id)?;
            if let Some(PositionIndex(positon_id)) = self.position_index {
                write!(f, "#{positon_id}")?;
            }
            Ok(())
        }
    }
}

impl From<&Chunk> for ProgressAt {
    fn from(chunk: &Chunk) -> ProgressAt {
        ProgressAt {
            batch_id: chunk.work.id(),
            batch_url: chunk.positions.last().and_then(|pos| pos.url.clone()),
            position_index: chunk.positions.last().and_then(|pos| pos.position_index),
        }
    }
}

impl From<&Position> for ProgressAt {
    fn from(pos: &Position) -> ProgressAt {
        ProgressAt {
            batch_id: pos.work.id(),
            batch_url: pos.url.clone(),
            position_index: pos.position_index,
        }
    }
}

impl From<&PositionResponse> for ProgressAt {
    fn from(pos: &PositionResponse) -> ProgressAt {
        ProgressAt {
            batch_id: pos.work.id(),
            batch_url: pos.url.clone(),
            position_index: pos.position_index,
        }
    }
}

struct LoggerState {
    pub progress_line: usize,
    /// Payload passed to `fishnet_info` (without `><> ` prefix) for deduplication.
    last_fishnet_line: Option<String>,
    /// After printing `.` for a repeated fishnet line, stdout has no trailing newline yet.
    fishnet_mid_line: bool,
}

impl LoggerState {
    fn end_fishnet_dot_run_if_needed(&mut self, stderr: bool) {
        if !self.fishnet_mid_line {
            return;
        }
        self.fishnet_mid_line = false;
        if stderr {
            writeln!(io::stderr()).nevermind("log");
        } else {
            writeln!(io::stdout()).nevermind("log");
        }
        if !stderr {
            io::stdout().flush().nevermind("flush");
        } else {
            io::stderr().flush().nevermind("flush");
        }
    }

    fn line_feed(&mut self) {
        if self.progress_line > 0 {
            self.progress_line = 0;
            writeln!(io::stdout()).nevermind("log to stdout");
        }
    }
}

pub struct QueueStatusBar {
    pub pending: usize,
    pub cores: NonZeroUsize,
}

impl fmt::Display for QueueStatusBar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let width = 16;
        let virtual_width = max(self.cores.get() * 3, 16);
        let cores_width = self.cores.get() * width / virtual_width;
        let pending_width = self.pending * width / virtual_width;
        let overhang_width = pending_width.saturating_sub(cores_width);
        let empty_width = width
            .checked_sub(cores_width)
            .and_then(|w| w.checked_sub(overhang_width));

        f.write_str("[")?;
        f.write_str(&"=".repeat(min(pending_width, cores_width)))?;
        f.write_str(&" ".repeat(cores_width.saturating_sub(pending_width)))?;
        f.write_str("|")?;
        f.write_str(&"=".repeat(min(overhang_width, width.saturating_sub(cores_width))))?;
        f.write_str(&" ".repeat(empty_width.unwrap_or(0)))?;
        f.write_str(if empty_width.is_none() { ">" } else { "]" })
    }
}

pub fn short_variant_name(variant: Variant) -> Option<&'static str> {
    Some(match variant {
        Variant::Antichess => "anti",
        Variant::Atomic => "atomic",
        Variant::Crazyhouse => "zh",
        Variant::Horde => "horde",
        Variant::KingOfTheHill => "koth",
        Variant::RacingKings => "race",
        Variant::ThreeCheck => "3check",
        Variant::Chess => return None,
    })
}
