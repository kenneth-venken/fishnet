#![deny(unsafe_code)]

mod api;
mod assets;
mod configure;
mod ipc;
mod logger;
mod queue;
mod stats;
mod stockfish;
mod systemd;
mod update;
mod util;

use std::{
    env, io,
    io::IsTerminal as _,
    num::NonZeroUsize,
    path::PathBuf,
    process,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use reqwest::Client;
use shell_escape::escape;
use sysinfo::System;
use tokio::{
    signal,
    sync::{mpsc, oneshot},
    task::JoinSet,
    time::{sleep, sleep_until},
};

use crate::{
    assets::{Assets, ByEngineFlavor, Cpu, EngineFlavor},
    configure::{Command, CpuPriority, Opt},
    ipc::{Chunk, ChunkFailed, EngineProgress, Pull},
    logger::{Logger, ProgressAt},
    update::{UpdateSuccess, auto_update},
    util::{RandomizedBackoff, dot_thousands},
};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let client = configure_client();
    let opt = configure::parse_and_configure(&client).await;
    let logger = Logger::new(opt.verbose, opt.command.is_some_and(Command::is_systemd));

    if opt.auto_update {
        let current_exe = env::current_exe().expect("current exe");
        match auto_update(
            !opt.command.is_some_and(Command::is_systemd),
            &client,
            &logger,
        )
        .await
        {
            Err(err) => logger.error(&format!("Failed to update: {err}")),
            Ok(UpdateSuccess::UpToDate(version)) => {
                logger.fishnet_info(&format!("Fishnet v{version} is up to date"));
            }
            Ok(UpdateSuccess::Updated(version)) => {
                logger.fishnet_info(&format!("Fishnet updated to v{version}"));
                restart_process(current_exe, &logger);
            }
        }
    }

    match opt.command {
        Some(Command::Run) | None => run(opt, &client, &logger).await,
        Some(Command::Systemd) => systemd::systemd_system(opt),
        Some(Command::SystemdUser) => systemd::systemd_user(opt),
        Some(Command::Configure) => (),
        Some(Command::License) => license(&logger),
    }
}

async fn run(opt: Opt, client: &Client, logger: &Logger) {
    logger.headline("Checking configuration ...");

    let endpoint = opt.endpoint();
    logger.info(&format!("Endpoint: {endpoint}"));

    logger.info(&format!(
        "Backlog: Join queue if user backlog >= {:?} or system backlog >= {:?}",
        Duration::from(opt.backlog.user.unwrap_or_default()),
        Duration::from(opt.backlog.system.unwrap_or_default())
    ));

    let cpu = Cpu::detect();
    logger.info(&format!("CPU features: {cpu}"));

    let assets = Assets::prepare(cpu).expect("prepared bundled stockfish");
    logger.info(&format!(
        "Engines: {}, {} (for GPLv3, run: {} license)",
        assets.stockfish.official.name,
        assets.stockfish.multi_variant.name,
        escape(
            env::args_os()
                .next()
                .and_then(|exe| exe.into_string().ok())
                .unwrap_or("./fishnet".to_owned())
                .into()
        )
    ));

    // Max threads from CPU: reserve 2 for OS.
    let max_threads_cpu: usize = thread::available_parallelism()
        .map(|n| (n.get().saturating_sub(2)).max(1))
        .unwrap_or(1);

    // Max threads from RAM: start from 70% of physical RAM, minimum 128 MB per thread.
    let ram_mb: usize = {
        let mut sys = System::new_all();
        sys.refresh_memory();
        (sys.total_memory() / (1024 * 1024)) as usize
    };
    let default_available_mem_mb = (ram_mb * 70) / 100;

    // Apply optional memory_limit (in MB) from CLI/ini as a cap:
    // - cannot exceed the automatic 70% of physical RAM
    // - can only reduce it
    let engine_mem_mb = opt
        .memory_limit
        .unwrap_or(default_available_mem_mb)
        .min(default_available_mem_mb);

    let max_threads_ram = (engine_mem_mb / 128).max(1);

    // Combine CPU and RAM limits, then apply optional thread_limit cap from CLI/ini.
    let mut total_threads = max_threads_cpu.min(max_threads_ram);
    if let Some(thread_limit) = opt.thread_limit {
        if thread_limit > 0 {
            total_threads = total_threads.min(thread_limit);
        }
    }

    logger.info(&format!(
        "Resources: {} MB RAM (70% = {} MB, engine budget = {} MB), max {} threads (CPU), max {} (RAM @ 128 MB/thread) → {} threads",
        ram_mb, default_available_mem_mb, engine_mem_mb, max_threads_cpu, max_threads_ram, total_threads
    ));

    let requested_cores = opt
        .cores
        .map(|c| c.number())
        .unwrap_or(NonZeroUsize::new(1).unwrap());

    let (cores, thread_distribution) = if requested_cores.get() > total_threads {
        logger.warn(&format!(
            "Requested {} engines but only {} threads available. Starting at most {} engines.",
            requested_cores,
            total_threads,
            total_threads
        ));
        let capped = NonZeroUsize::new(total_threads).unwrap_or(NonZeroUsize::MIN);
        let dist = distribute_threads(total_threads, capped.get());
        (capped, dist)
    } else {
        let dist = distribute_threads(total_threads, requested_cores.get());
        (requested_cores, dist)
    };

    let per_thread_mb = (engine_mem_mb / total_threads).max(128);
    let hash_distribution: Vec<usize> = thread_distribution
        .iter()
        .map(|&t| per_thread_mb * t)
        .collect();

    logger.info(&format!(
        "maxPV concurrency: {} engine(s), threads: {:?}, hash (MB): {:?} ({} MB/thread)",
        cores,
        thread_distribution,
        hash_distribution,
        per_thread_mb
    ));

    // Install handler for SIGTERM.
    #[cfg(unix)]
    let mut sig_term = signal::unix::signal(signal::unix::SignalKind::terminate())
        .expect("install handler for sigterm");
    #[cfg(windows)]
    let mut sig_term = signal::windows::ctrl_break().expect("install handler for ctrl+break");

    // Install handler for SIGINT.
    #[cfg(unix)]
    let mut sig_int = signal::unix::signal(signal::unix::SignalKind::interrupt())
        .expect("install handler for sigint");
    #[cfg(windows)]
    let mut sig_int = signal::windows::ctrl_c().expect("install handler for ctrl+c");

    // To wait for workers and API actor before shutdown.
    let mut join_set = JoinSet::new();

    // Spawn API actor.
    let (api, api_actor) = api::channel(endpoint.clone(), opt.key, client.clone(), logger.clone());
    join_set.spawn(api_actor.run());

    let to_stop = if io::stdout().is_terminal() {
        "CTRL-C"
    } else {
        "SIGINT"
    };
    logger.headline(&format!("Running ({to_stop} to stop) ..."));

    // Spawn queue actor.
    let (mut queue, queue_actor, progress_tx) = queue::channel(
        opt.stats,
        opt.backlog,
        cores,
        api,
        opt.max_backoff.unwrap_or_default(),
        logger.clone(),
    );
    join_set.spawn(queue_actor.run());

    // Spawn workers. Workers handle engine processes and send their results
    // to tx, thereby requesting more work.
    let mut rx = {
        let assets = Arc::new(assets);
        let (tx, rx) = mpsc::channel::<Pull>(cores.get());
        for i in 0..cores.get() {
            let assets = assets.clone();
            let tx = tx.clone();
            let logger = logger.clone();
            let threads = thread_distribution[i];
            let hash_mb = hash_distribution[i];
            let progress_tx = progress_tx.clone();
            join_set.spawn(worker(
                i,
                assets,
                tx,
                logger,
                threads,
                hash_mb,
                progress_tx,
            ));
        }
        rx
    };

    // Set scheduling priority.
    match opt.cpu_priority.unwrap_or_default() {
        CpuPriority::Unchanged => (),
        CpuPriority::Min => {
            if let Err(err) = set_current_process_min_priority() {
                logger.warn(&format!("Failed to decrease CPU priority: {err:?}"));
            }
        }
    }

    let mut restart = None;
    let mut up_to_date = Instant::now();
    let mut summarized = Instant::now();
    let mut shutdown_soon = false;

    loop {
        // Check for updates from time to time.
        let now = Instant::now();
        if opt.auto_update
            && !shutdown_soon
            && now.duration_since(up_to_date) >= Duration::from_secs(60 * 60 * 5)
        {
            up_to_date = now;
            let current_exe = env::current_exe().expect("current exe");
            match auto_update(false, client, logger).await {
                Err(err) => logger.error(&format!("Failed to update in the background: {err}")),
                Ok(UpdateSuccess::UpToDate(version)) => {
                    logger.fishnet_info(&format!("Fishnet v{version} is up to date"));
                }
                Ok(UpdateSuccess::Updated(version)) => {
                    logger
                        .fishnet_info(&format!("Fishnet updated to v{version}. Will restart soon"));
                    restart = Some(current_exe);
                    shutdown_soon = true;
                    queue.shutdown_soon().await;
                }
            }
        }

        // Print summary from time to time.
        if now.duration_since(summarized) >= Duration::from_secs(120) {
            summarized = now;
            let (stats, nnue_nps) = queue.stats().await;
            logger.fishnet_info(&format!(
                "v{}: {} (nnue), {} batches, {} positions, {} total nodes",
                env!("CARGO_PKG_VERSION"),
                nnue_nps,
                dot_thousands(stats.total_batches),
                dot_thousands(stats.total_positions),
                dot_thousands(stats.total_nodes),
            ));
        }

        // Main loop. Handles signals, forwards worker results from rx to the
        // queue and responds with more work.
        tokio::select! {
            res = sig_int.recv() => {
                res.expect("sigint handler installed");
                logger.clear_echo();
                if shutdown_soon {
                    logger.fishnet_info("Stopping now.");
                    rx.close();
                } else {
                    logger.headline(&format!("Stopping soon. {to_stop} again to abort pending batches ..."));
                    queue.shutdown_soon().await;
                    shutdown_soon = true;
                }
            }
            res = sig_term.recv() => {
                res.expect("sigterm handler installed");
                logger.fishnet_info("Stopping now.");
                shutdown_soon = true;
                rx.close();
            }
            res = rx.recv() => {
                if let Some(res) = res {
                    queue.pull(res).await;
                } else {
                    logger.debug("About to exit.");
                    break;
                }
            }
            _ = sleep(Duration::from_secs(120)) => (),
        }
    }

    // Shutdown queue to abort remaining chunks.
    queue.shutdown().await;

    // Drop the last progress sender so work-progress and workers can finish.
    // Keeping it alive deadlocks shutdown: work_progress_loop holds an ApiStub,
    // join_set waits for the API actor, and the API actor waits for that stub.
    drop(progress_tx);

    // Wait for workers, queue actor, API actor, and engine tasks.
    while let Some(res) = join_set.join_next().await {
        res.expect("join");
    }

    // Restart.
    if let Some(restart) = restart.take() {
        restart_process(restart, logger);
    }
}

/// Distributes `total_threads` evenly across `num_engines`.
/// Returns a vec of length `num_engines` that sums to `total_threads`
/// (e.g. 10 threads, 3 engines → [4, 3, 3]; 32 threads, 7 engines → [5, 5, 5, 5, 4, 4, 4]).
fn distribute_threads(total_threads: usize, num_engines: usize) -> Vec<usize> {
    assert!(num_engines >= 1 && num_engines <= total_threads);
    let base = total_threads / num_engines;
    let remainder = total_threads % num_engines;
    (0..num_engines)
        .map(|i| base + if i < remainder { 1 } else { 0 })
        .collect()
}

async fn worker(
    i: usize,
    assets: Arc<Assets>,
    tx: mpsc::Sender<Pull>,
    logger: Logger,
    threads: usize,
    hash_mb: usize,
    progress_tx: mpsc::Sender<EngineProgress>,
) {
    logger.debug(&format!("Started worker {i}."));

    let mut chunk: Option<Chunk> = None;
    let mut engine = ByEngineFlavor {
        official: None,
        multi_variant: None,
    };
    let mut engine_backoff = RandomizedBackoff::default();

    loop {
        let responses = if let Some(chunk) = chunk.take() {
            // Ensure engine process is ready.
            let flavor = chunk.flavor;
            let context = ProgressAt::from(&chunk);
            let (mut sf, join_handle) = if let Some((sf, join_handle)) =
                engine.get_mut(flavor).take()
            {
                (sf, join_handle)
            } else {
                // Backoff before starting engine.
                let backoff = engine_backoff.next();
                if backoff >= Duration::from_secs(5) {
                    logger.info(&format!(
                        "Waiting {backoff:?} before attempting to start engine"
                    ));
                } else {
                    logger.debug(&format!(
                        "Waiting {backoff:?} before attempting to start engine"
                    ));
                }
                tokio::select! {
                    _ = tx.closed() => break,
                    _ = sleep(engine_backoff.next()) => (),
                }

                // Start engine and spawn actor.
                let (sf, sf_actor) = stockfish::channel(
                    assets.stockfish.get(flavor).path.clone(),
                    logger.clone(),
                    threads,
                    hash_mb,
                    progress_tx.clone(),
                );
                let join_handle = tokio::spawn(sf_actor.run());
                (sf, join_handle)
            };

            // Analyse or play.
            let batch_id = chunk.work.id();
            let res = tokio::select! {
                _ = tx.closed() => {
                    logger.debug(&format!("Worker {i} shutting down engine early"));
                    drop(sf);
                    join_handle.await.expect("join");
                    break;
                }
                _ = sleep_until(chunk.deadline) => {
                    logger.warn(&match flavor {
                        EngineFlavor::Official => format!("Official Stockfish timed out in worker {i}. If this happens frequently it is better to stop and defer to clients with better hardware. Context: {context}"),
                        EngineFlavor::MultiVariant => format!("Fairy-Stockfish timed out in worker {i}. Context: {context}"),
                    });
                    drop(sf);
                    join_handle.await.expect("join");
                    Err(ChunkFailed { batch_id })
                }
                res = sf.go_multiple(chunk) => {
                    match res {
                        Ok(res) => {
                            *engine.get_mut(flavor) = Some((sf, join_handle));
                            engine_backoff.reset();
                            Ok(res)
                        }
                        Err(failed) => {
                            drop(sf);
                            logger.warn(&format!("Worker {i} waiting for engine to shut down after error. Context: {context}"));
                            join_handle.await.expect("join");
                            Err(failed)
                        },
                    }
                }
            };

            res
        } else {
            Ok(Vec::new())
        };

        let (callback, waiter) = oneshot::channel();

        if tx
            .send(Pull {
                responses,
                callback,
            })
            .await
            .is_err()
        {
            logger.debug(&format!(
                "Worker {i} was about to send result, but shutting down"
            ));
            break;
        }

        tokio::select! {
            _ = tx.closed() => break,
            res = waiter => {
                match res {
                    Ok(next_chunk) => chunk = Some(next_chunk),
                    Err(_) => break,
                }
            }
        }
    }

    if let Some((sf, join_handle)) = engine.get_mut(EngineFlavor::Official).take() {
        logger.debug(&format!(
            "Worker {i} waiting for standard engine to shut down"
        ));
        drop(sf);
        join_handle.await.expect("join");
    }

    if let Some((sf, join_handle)) = engine.get_mut(EngineFlavor::MultiVariant).take() {
        logger.debug(&format!(
            "Worker {i} waiting for multi-variant engine to shut down"
        ));
        drop(sf);
        join_handle.await.expect("join");
    }

    logger.debug(&format!("Stopped worker {i}"));
    drop(tx);
}

fn license(logger: &Logger) {
    logger.headline("LICENSE.txt");
    println!("{}", include_str!("../LICENSE.txt"));
    logger.headline("COPYING.txt");
    print!("{}", include_str!("../COPYING.txt"));
}

fn restart_process(current_exe: PathBuf, logger: &Logger) {
    logger.headline(&format!("Waiting 5s before restarting {current_exe:?} ..."));
    thread::sleep(Duration::from_secs(5));
    let err = exec(process::Command::new(current_exe).args(std::env::args_os().skip(1)));
    panic!("Failed to restart: {err}");
}

#[cfg(unix)]
fn exec(command: &mut process::Command) -> io::Error {
    use std::os::unix::process::CommandExt as _;
    // Completely replace the current process image. If successful, execution
    // of the current process stops here.
    command.exec()
}

#[cfg(windows)]
fn exec(command: &mut process::Command) -> io::Error {
    use std::os::windows::process::CommandExt as _;
    // No equivalent for Unix exec() exists. So create a new independent
    // console instead and terminate the current one:
    // https://docs.microsoft.com/en-us/windows/win32/procthread/process-creation-flags
    let create_new_console = 0x0000_0010;
    match command.creation_flags(create_new_console).spawn() {
        Ok(_) => process::exit(0),
        Err(err) => return err,
    }
}

#[cfg(unix)]
#[allow(unsafe_code)]
fn set_current_process_min_priority() -> io::Result<()> {
    use libc::{PRIO_PROCESS, setpriority};

    // On Linux the priority range is -20 (highest) to 19 (lowest). On other
    // Unixes the range is -20 to 20.
    #[cfg(target_os = "linux")]
    const MINIMUM_PRIORITY_NICENESS: libc::c_int = 19;
    #[cfg(not(target_os = "linux"))]
    const MINIMUM_PRIORITY_NICENESS: libc::c_int = 20;

    if unsafe { setpriority(PRIO_PROCESS, 0, MINIMUM_PRIORITY_NICENESS) != 0 } {
        return Err(io::Error::last_os_error());
    }

    Ok(())
}

#[cfg(windows)]
#[allow(unsafe_code)]
fn set_current_process_min_priority() -> windows::core::Result<()> {
    use windows::Win32::System::Threading::{
        BELOW_NORMAL_PRIORITY_CLASS, GetCurrentProcess, SetPriorityClass,
    };

    // BELOW_NORMAL_PRIORITY_CLASS is the lowest priority that won't completely
    // starve tasks of CPU time on high loads. The lowest IDLE_PRIORITY_CLASS
    // is stricter than Linux's nice 19!
    unsafe { SetPriorityClass(GetCurrentProcess(), BELOW_NORMAL_PRIORITY_CLASS) }
}

fn configure_client() -> Client {
    let http_timeout = env::var("FISHNET_HTTP_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(30));

    let http_connect_timeout = env::var("FISHNET_HTTP_CONNECT_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(10));

    // Build TLS backend that supports SSLKEYLOGFILE.
    let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("default tls versions supported")
    .with_root_certificates(rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    })
    .with_no_client_auth();

    tls.alpn_protocols = vec!["h2".into(), "http/1.1".into()];
    tls.key_log = Arc::new(rustls::KeyLogFile::new());

    // Configure client.
    Client::builder()
        .user_agent(format!(
            "{}-{}-{}/{}",
            env!("CARGO_PKG_NAME"),
            env::consts::OS,
            env::consts::ARCH,
            env!("CARGO_PKG_VERSION")
        ))
        .connect_timeout(http_connect_timeout)
        .timeout(http_timeout)
        .pool_idle_timeout(Duration::from_secs(25))
        .use_preconfigured_tls(tls)
        .build()
        .expect("client")
}
