use std::{env, io, mem, num::NonZeroU8, path::PathBuf, process::Stdio, str::FromStr, time::Duration};

use shakmaty::uci::UciMove;
use tokio::{
    io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader, BufWriter, Lines},
    process::{ChildStdin, ChildStdout, Command},
    sync::{mpsc, oneshot},
};

use crate::{
    api::{Score, Work},
    assets::{EngineFlavor, EvalFlavor},
    ipc::{Chunk, ChunkFailed, Matrix, Position, PositionResponse},
    logger::{Logger, ProgressAt},
    util::NevermindExt as _,
};

pub fn channel(mut exe: PathBuf, logger: Logger) -> (StockfishStub, StockfishActor) {
    // 1. Highest priority: env var
    if let Ok(path) = env::var("STOCKFISH_PATH") {
        exe = PathBuf::from(path);
        logger.info(&format!("Using STOCKFISH_PATH env: {}", exe.display()));
    }
    // 2. Second priority: engine= from fishnet.ini
    else if let Ok(content) = std::fs::read_to_string("fishnet.ini") {
        for line in content.lines() {
            let line = line.trim();
            if line.starts_with("engine=") || line.starts_with("engine =") {
                if let Some(eq) = line.find('=') {
                    let value = line[eq + 1..].trim().trim_matches(|c| c == '"' || c == '\'');
                    if !value.is_empty() {
                        exe = PathBuf::from(value);
                        logger.info(&format!("Loaded engine from fishnet.ini: {}", value));
                        break;
                    }
                }
            }
        }
    }
    // 3. Fallback: CWD
    else if let Some(name) = exe.file_name() {
         let cwd_path = std::env::current_dir().unwrap_or_default().join(name);
         if cwd_path.exists() {
             exe = cwd_path;
             logger.info(&format!("Using engine from CWD: {}", exe.display()));
         }
     }

     // Make absolute path (safer for spawn)
     if let Ok(abs) = exe.canonicalize() {
         exe = abs;
     }

    let (tx, rx) = mpsc::channel(1);
    (
        StockfishStub { tx },
        StockfishActor {
            rx,
            exe,
            initialized: false,
            logger,
        },
    )
}

pub struct StockfishStub {
    tx: mpsc::Sender<StockfishMessage>,
}

impl StockfishStub {
    pub async fn go_multiple(
        &mut self,
        chunk: Chunk,
    ) -> Result<Vec<PositionResponse>, ChunkFailed> {
        let (callback, responses) = oneshot::channel();
        let batch_id = chunk.work.id();
        self.tx
            .send(StockfishMessage::GoMultiple { chunk, callback })
            .await
            .map_err(|_| ChunkFailed { batch_id })?;
        responses.await.map_err(|_| ChunkFailed { batch_id })
    }
}

pub struct StockfishActor {
    rx: mpsc::Receiver<StockfishMessage>,
    exe: PathBuf,
    initialized: bool,
    logger: Logger,
}

#[derive(Debug)]
enum StockfishMessage {
    GoMultiple {
        chunk: Chunk,
        callback: oneshot::Sender<Vec<PositionResponse>>,
    },
}

struct Stdout {
    inner: Lines<BufReader<ChildStdout>>,
}

impl Stdout {
    fn new(inner: ChildStdout) -> Stdout {
        Stdout {
            inner: BufReader::new(inner).lines(),
        }
    }

    async fn read_line(&mut self) -> io::Result<String> {
        if let Some(line) = self.inner.next_line().await? {
            Ok(line)
        } else {
            Err(io::ErrorKind::UnexpectedEof.into())
        }
    }
}

struct Stdin {
    inner: BufWriter<ChildStdin>,
}

impl Stdin {
    fn new(inner: ChildStdin) -> Stdin {
        Stdin {
            inner: BufWriter::new(inner),
        }
    }

    async fn write_line(&mut self, line: &str) -> io::Result<()> {
        self.inner.write_all(line.as_bytes()).await?;
        self.inner.write_all(b"\n").await
    }

    async fn flush(&mut self) -> io::Result<()> {
        self.inner.flush().await
    }
}

#[derive(Debug)]
enum EngineError {
    IoError(io::Error),
    Shutdown,
}

impl From<io::Error> for EngineError {
    fn from(error: io::Error) -> EngineError {
        EngineError::IoError(error)
    }
}

fn new_process_group(command: &mut Command) -> &mut Command {
    #[cfg(unix)]
    {
        // Stop SIGINT from propagating to child process.
        command.process_group(0);
    }

    #[cfg(windows)]
    {
        // Stop CTRL+C from propagating to child process:
        // https://docs.microsoft.com/en-us/windows/win32/procthread/process-creation-flags
        let create_new_process_group = 0x0000_0200;
        command.creation_flags(create_new_process_group);
    }

    command
}

impl StockfishActor {
    pub async fn run(self) {
        let logger = self.logger.clone();
        if let Err(EngineError::IoError(err)) = self.run_inner().await {
            logger.error(&format!("Engine error: {err}"));
        }
    }

    async fn run_inner(mut self) -> Result<(), EngineError> {
        // === AUTOMATIC CWD FALLBACK ===
        if !self.exe.exists() {
            if let Some(name) = self.exe.file_name() {
                let cwd_exe = std::env::current_dir().unwrap_or_default().join(name);
                if cwd_exe.exists() {
                    self.logger.info(&format!("✅ Fallback to CWD engine: {}", cwd_exe.display()));
                    self.exe = cwd_exe;
                } else {
                    self.logger.error(&format!("❌ Engine not found in configured path or CWD: {}", self.exe.display()));
                    return Err(EngineError::IoError(io::Error::new(io::ErrorKind::NotFound, "engine not found")));
                }
            }
        } else {
            self.logger.debug(&format!("✅ Engine file EXISTS on disk: {}", self.exe.display()));
        }

        let mut child = new_process_group(&mut Command::new(&self.exe))
            .current_dir(self.exe.parent().expect("absolute path"))
            .stdout(Stdio::piped())
            .stdin(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;

        let pid = child.id().expect("pid");
        let mut stdout = Stdout::new(
            child
                .stdout
                .take()
                .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "stdout closed"))?,
        );
        let mut stdin = Stdin::new(
            child
                .stdin
                .take()
                .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "stdin closed"))?,
        );

        loop {
            tokio::select! {
                msg = self.rx.recv() => {
                    if let Some(msg) = msg {
                        self.handle_message(&mut stdout, &mut stdin, msg).await?;
                    } else {
                        break;
                    }
                }
                status = child.wait() => {
                    match status? {
                        status if status.success() => {
                            self.logger.debug(&format!("Stockfish process {pid} exited with status {status}"));
                        }
                        status => {
                            self.logger.error(&format!("Stockfish process {pid} exited with status {status}"));
                        }
                    }
                    break;
                }
            }
        }

        Ok(())
    }

    async fn handle_message(
        &mut self,
        stdout: &mut Stdout,
        stdin: &mut Stdin,
        msg: StockfishMessage,
    ) -> Result<(), EngineError> {
        match msg {
            StockfishMessage::GoMultiple {
                mut callback,
                chunk,
            } => {
                tokio::select! {
                    _ = callback.closed() => Err(EngineError::Shutdown),
                    res = self.go_multiple(stdout, stdin, chunk) => {
                        callback.send(res?).nevermind("go receiver dropped");
                        Ok(())
                    }
                }
            }
        }
    }

    async fn init(&mut self, stdout: &mut Stdout, stdin: &mut Stdin) -> io::Result<()> {
        if !mem::replace(&mut self.initialized, true) {
            stdin
                .write_line("setoption name UCI_Chess960 value true")
                .await?;

            // === MAXPV THREAD ALLOCATION (jouw verzoek) ===
            let cores = num_cpus::get();
            let threads = (cores.saturating_sub(2)).max(1);  // reserveer 2 cores voor OS
            let hash_mb = match cores {
                c if c >= 16 => 16384,
                c if c >= 8  => 8192,
                c if c >= 4  => 4096,
                _            => 2048,
            };

            self.logger.info(&format!(
                "🚀 maxPV engine gestart | {} cores → {} threads + {}MB Hash (concurrency=1 aanbevolen)",
                cores, threads, hash_mb
            ));

            stdin.write_line(&format!("setoption name Threads value {}", threads)).await?;
            stdin.write_line(&format!("setoption name Hash value {}", hash_mb)).await?;
            stdin.write_line("setoption name UCI_AnalyseMode value true").await?;
            stdin.write_line("setoption name Use NNUE value true").await?;
            stdin.write_line("setoption name Contempt value 0").await?;
            stdin.write_line("setoption name SlowMover value 100").await?;
            stdin.write_line("clear hash").await?;

            stdin.write_line("isready").await?;
            stdin.flush().await?;

            loop {
                let line = stdout.read_line().await?;
                if line.trim_end() == "readyok" {
                    self.logger.debug("Engine is ready with maxPV settings");
                    break;
                } else if !line.starts_with("Stockfish ") && !line.starts_with("Fairy-Stockfish ") {
                    self.logger.warn(&format!(
                        "Unexpected engine initialization output: {}",
                        line.trim_end()
                    ));
                }
            }
        }
        Ok(())
    }

    async fn go_multiple(
        &mut self,
        stdout: &mut Stdout,
        stdin: &mut Stdin,
        chunk: Chunk,
    ) -> io::Result<Vec<PositionResponse>> {
        // Set global options (once).
        self.init(stdout, stdin).await?;

        // Clear hash.
        stdin.write_line("ucinewgame").await?;

        // Set basic options.
        if chunk.flavor == EngineFlavor::MultiVariant {
            stdin
                .write_line(&format!(
                    "setoption name Use NNUE value {}",
                    chunk.flavor.eval_flavor().is_nnue()
                ))
                .await?;
            stdin
                .write_line(&format!(
                    "setoption name UCI_AnalyseMode value {}",
                    matches!(chunk.work, Work::Analysis { .. })
                ))
                .await?;
            stdin
                .write_line(&format!(
                    "setoption name UCI_Variant value {}",
                    chunk.variant.uci()
                ))
                .await?;
        }
        stdin
            .write_line(&format!(
                "setoption name MultiPV value {}",
                chunk.work.multipv()
            ))
            .await?;
        stdin
            .write_line(&format!(
                "setoption name Skill Level value {}",
                match chunk.work {
                    Work::Analysis { .. } => 20,
                    Work::Move { level, .. } => level.skill_level(),
                }
            ))
            .await?;

        // Collect results for all positions of the chunk.
        let mut responses = Vec::with_capacity(chunk.positions.len());
        for position in chunk.positions {
            responses.push(
                self.go(stdout, stdin, chunk.flavor.eval_flavor(), position)
                    .await?,
            );
        }
        Ok(responses)
    }

    async fn go(
        &mut self,
        stdout: &mut Stdout,
        stdin: &mut Stdin,
        eval_flavor: EvalFlavor,
        position: Position,
    ) -> io::Result<PositionResponse> {
        // Setup position.
        let moves = position
            .moves
            .iter()
            .map(|m| m.to_string())
            .collect::<Vec<_>>()
            .join(" ");
        stdin
            .write_line(&format!(
                "position fen {} moves {}",
                position.root_fen, moves
            ))
            .await?;

        // Go.
        let go = match &position.work {
            Work::Move { level, clock, .. } => {
                let mut go = vec![
                    "go".to_owned(),
                    "movetime".to_owned(),
                    level.time().as_millis().to_string(),
                    "depth".to_owned(),
                    level.depth().to_string(),
                ];

                if let Some(clock) = clock {
                    go.extend_from_slice(&[
                        "wtime".to_owned(),
                        Duration::from(clock.wtime).as_millis().to_string(),
                        "btime".to_owned(),
                        Duration::from(clock.btime).as_millis().to_string(),
                        "winc".to_owned(),
                        clock.inc.as_millis().to_string(),
                        "binc".to_owned(),
                        clock.inc.as_millis().to_string(),
                    ]);
                }

                go
            }

            Work::Analysis { nodes, depth, .. } => {
                let mut go = vec!["go".to_owned()];

                if let Some(d) = depth {
                    // MAXPV deep analysis: only depth, NO nodes limit, NO timeout
                    go.extend_from_slice(&["depth".to_owned(), d.to_string()]);
                    self.logger.info(&format!("Starting deep analysis → go depth {}", d));
                } else {
                    // Old Lichess nodes behavior: only nodes
                    let nodes =
                        nodes.expect("analysis work must provide either depth or nodes");
                    go.extend_from_slice(&[
                        "nodes".to_owned(),
                        nodes.get(eval_flavor).to_string(),
                    ]);
                }
                go
            }
        };
        stdin.write_line(&go.join(" ")).await?;
        stdin.flush().await?;

        // Process response.
        let mut scores = Matrix::new();
        let mut pvs = Matrix::new();
        let mut latest_depth = 0;
        let mut latest_time = Duration::default();
        let mut latest_nodes = 0;
        let mut latest_nps = None;

        loop {
            let line = stdout.read_line().await?.parse()?;
            match line {
                UciLine::Bestmove(best_move) => {
                    if scores.best().is_none() {
                        return Err(io::Error::new(io::ErrorKind::InvalidData, "missing score"));
                    }
                    return Ok(PositionResponse {
                        work: position.work,
                        position_index: position.position_index,
                        url: position.url,
                        best_move,
                        scores,
                        depth: latest_depth,
                        pvs,
                        time: latest_time,
                        nodes: latest_nodes,
                        nps: latest_nps,
                    });
                }
                UciLine::Info {
                    multipv,
                    depth,
                    nodes,
                    time,
                    nps,
                    score,
                    lowerbound,
                    upperbound,
                    pv,
                } => {
                    if let Some(depth) = depth
                        && multipv.get() == 1
                        && !lowerbound
                        && !upperbound
                    {
                        latest_depth = depth;
                    }
                    if let Some(nodes) = nodes {
                        latest_nodes = nodes;
                    }
                    if let Some(time) = time {
                        latest_time = time;
                    }
                    latest_nps = nps.or(latest_nps);
                    if let Some(score) = score
                        && let Some(depth) = depth
                        && ((!lowerbound && !upperbound) || multipv.get() > 1)
                    {
                        if !score.is_plausible() {
                            self.logger.warn(&format!(
                                "Implausible score {:?} at depth {}, context: {}",
                                score,
                                depth,
                                ProgressAt {
                                    batch_id: position.work.id(),
                                    batch_url: position.url.clone(),
                                    position_index: position.position_index,
                                }
                            ));
                        }
                        scores.set(multipv, depth, score);
                    }
                    if let Some(pv) = pv
                        && let Some(depth) = depth
                        && ((!lowerbound && !upperbound) || multipv.get() > 1)
                    {
                        pvs.set(multipv, depth, pv);
                    }
                }
            }
        }
    }
}

enum UciLine {
    Bestmove(Option<UciMove>),
    Info {
        multipv: NonZeroU8,
        depth: Option<u8>,
        nodes: Option<u64>,
        time: Option<Duration>,
        nps: Option<u32>,
        score: Option<Score>,
        lowerbound: bool,
        upperbound: bool,
        pv: Option<Vec<UciMove>>,
    },
}

impl FromStr for UciLine {
    type Err = io::Error;

    fn from_str(line: &str) -> io::Result<UciLine> {
        let mut parts = line.split(' ');
        Ok(match parts.next() {
            Some("bestmove") => UciLine::Bestmove(parts.next().and_then(|m| m.parse().ok())),
            Some("info") => {
                let mut multipv = NonZeroU8::MIN;
                let mut depth = None;
                let mut nodes = None;
                let mut time = None;
                let mut nps = None;
                let mut score = None;
                let mut lowerbound = false;
                let mut upperbound = false;
                let mut pv = None;
                while let Some(part) = parts.next() {
                    match part {
                        "multipv" => {
                            multipv =
                                parts.next().and_then(|t| t.parse().ok()).ok_or_else(|| {
                                    io::Error::new(io::ErrorKind::InvalidData, "expected multipv")
                                })?;
                        }
                        "depth" => {
                            depth = Some(parts.next().and_then(|t| t.parse().ok()).ok_or_else(
                                || io::Error::new(io::ErrorKind::InvalidData, "expected depth"),
                            )?);
                        }
                        "nodes" => {
                            nodes = Some(parts.next().and_then(|t| t.parse().ok()).ok_or_else(
                                || io::Error::new(io::ErrorKind::InvalidData, "expected nodes"),
                            )?);
                        }
                        "time" => {
                            time = Some(
                                parts
                                    .next()
                                    .and_then(|t| t.parse().ok())
                                    .map(Duration::from_millis)
                                    .ok_or_else(|| {
                                        io::Error::new(io::ErrorKind::InvalidData, "expected time")
                                    })?,
                            );
                        }
                        "nps" => {
                            nps = parts.next().and_then(|n| n.parse().ok());
                        }
                        "score" => {
                            score = Some(
                                match parts.next() {
                                    Some("cp") => {
                                        parts.next().and_then(|cp| cp.parse().ok()).map(Score::Cp)
                                    }
                                    Some("mate") => parts
                                        .next()
                                        .and_then(|mate| mate.parse().ok())
                                        .map(Score::Mate),
                                    _ => {
                                        return Err(io::Error::new(
                                            io::ErrorKind::InvalidData,
                                            "expected cp or mate",
                                        ));
                                    }
                                }
                                .ok_or_else(|| {
                                    io::Error::new(io::ErrorKind::InvalidData, "expected score")
                                })?,
                            );
                        }
                        "lowerbound" => {
                            lowerbound = true;
                        }
                        "upperbound" => {
                            upperbound = true;
                        }
                        "pv" => {
                            pv = Some(
                                (&mut parts)
                                    .map(|part| part.parse::<UciMove>())
                                    .collect::<Result<Vec<_>, _>>()
                                    .map_err(|_| {
                                        io::Error::new(io::ErrorKind::InvalidData, "invalid pv")
                                    })?,
                            );
                        }
                        "string" => break,
                        _ => (),
                    }
                }
                UciLine::Info {
                    multipv,
                    depth,
                    nodes,
                    time,
                    nps,
                    score,
                    lowerbound,
                    upperbound,
                    pv,
                }
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unexpected engine output: {line}"),
                ));
            }
        })
    }
}
