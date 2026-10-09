# uringserver — a header-only C++20 networking library for Linux, built directly on io_uring
TCP and UDP share one completion-driven core:**one io_uring per worker thread**,and every callback (TCP on_connected/on_recv/on_close,UDP on_recv/on_recv_view,IoUring::async_* completions) runs on the thread that owns the connection, so business code never deals with reactor thread-affinity by hand.
- Only external dependency: liburing.
- Requires: -std=c++20, liburing >= 2.4, Linux kernel >= 6.1.
- TCP: accept multishot (with automatic fallback), connection limit, idle timeout, keepalive,
  graceful drain and forced-close fallback, EMFILE self-healing, IPv4/IPv6.
- UDP: per-thread SO_REUSEPORT sockets, provided buffer ring + multishot zero-copy receive,
  in-flight send caps for backpressure, pause/resume without stale datagrams, IPv4/IPv6.
- Async logging (own ring, batched submit, size-based rotation) and built-in observability:
  in-flight requests, send backpressure drops, truncated datagrams, buffer-pool hit rate.

# 1. Directory Structure
```
uringserver
├──include
│  └──uringserver
│       ├──argparser.hpp        #ArgParser
│       ├──threadpool.hpp       #ThreadPool
│       ├──tcpserver.hpp        #Connection,TcpServer
│       ├──udpserver.hpp        #UdpServer
│       ├──iouring.hpp          #IoUring
│       ├──socket.hpp           #Socket
│       ├──buffer.hpp           #Buffer,BufferPool
│       └──log.hpp              #Log,TRACE/DEBUG/INFO/WARN/ERROR/FATAL
├──examples
│  └──echoserver
│      ├──echoclient.cc
│      ├──echoserver.cc
│      └──makefile
└──README.md
```

# 2. Architecture
## 2.1 IoUring
### 2.1.1 IoUring Architecture Diagram
```
    caller: any thread (a business thread,or a callback running on a worker)
                                        │
        async_read · async_write · async_recv · async_send · async_recvmsg
    async_sendmsg · async_accept · async_close · async_splice · async_poll_out
                post · add_timer · cancel_timer · cancel_fd
                                        │
                                        ▼
  ┌────────────────────────────────────────────────────────────────────────────┐
  │ submit_task()                                                              │
  │  1. new IoTask from the thread-local pool (TlsTaskCache, 512/thread,       │
  │     no lock, no malloc)                                                    │
  │  2. under _submit_mtx: take an SQE, prep by IoEvent, set_data(task)        │
  │  3. register the task in the in-flight table (shutdown safety net)         │
  │  4. worker-submitted requests are queued and flushed after this batch;     │
  │     external threads call io_uring_submit() at once                        │
  └────────────────────────────────────────────────────────────────────────────┘
                                        │ SQE (user_data = IoTask*)
                                        ▼
  ┌────────────────────────────────────────────────────────────────────────────┐
  │ kernel io_uring                                                            │
  │  SQ: 32768 entries (URING_ENTRIES)                                         │
  │  CQ: 65536 entries (kernel default: 2 x SQ)                                │
  │  CQE: res (byte count / fd / negative errno)                               │
  │       flags: F_MORE (more to come), F_BUFFER (buffer returned), bid        │
  └────────────────────────────────────────────────────────────────────────────┘
                                        │ CQE (user_data = IoTask*)
                                        ▼
  ┌────────────────────────────────────────────────────────────────────────────┐
  │ worker_loop(): one IoUring = one io_uring + one worker thread              │
  │                                                                            │
  │  while(state==Running&&!stop_requested)                                    │
  │  {                                                                         │
  │      flush_submit()        hand off the SQEs queued by callbacks in one go │
  │      wait_cqe_timeout(1s)  wait for completions; also the timer-wheel tick │
  │      peek_batch_cqe(128)   harvest in batches                              │
  │      process_cqes()        dispatch -> user callback (on this thread)      │
  │      cq_advance+flush_submit()                                             │
  │  }                                                                         │
  │  cleanup(): flush -> disarm timers -> drain -> cancel in-flight            │
  │             -> force-complete leftovers -> clear the timer wheel           │
  └──────────────┬────────────────────────────────────────────────┬────────────┘
                 ▼                                                ▼
  ┌────────────────────────────┐                     ┌─────────────────────────┐
  │ timer wheel: 60 slots, 1 s │                     │ per-ring private state  │
  │  · a 1 s multishot timeout │                     │  · in-flight task table │
  │    drives tick()           │                     │  · provided buffer ring │
  │  · add_timer(delay,cb)     │                     │  · buffer-ring memory   │
  │  · cancel_timer(id)        │                     │  · _retired/_forced     │
  └────────────────────────────┘                     └─────────────────────────┘
```
## 2.2 TcpServer
### 2.2.1 TcpServer Architecture Diagram
```
                      business threads(any thread)
        conn->send()/send_on_ring()/post()/shutdown()/consume()/metrics
                    worker threads (one per IoUring):
        on_connected/on_recv/on_close all run on the thread that owns
                the connection -- callbacks never migrate
                                    │
                                    ▼
  ┌───────────────────────────────────────────────────────────────────────────┐
  │ _socket          listening socket (non-blocking,SO_REUSEADDR)             │
  │ _urings          one IoUring + worker thread per slot; slot == index      │
  │ _conns           id -> shared_ptr<Connection>, guarded by _mutex_conns    │
  │ _accept_*        _accept_inflight[N] / _accept_err_streak[N], alignas(64) │
  │                  _accept_paused / _accept_multishot / _rescue_fd          │
  │ _limits          _max_connections / _connection_timeout / _keepalive      │
  │                  / _drain_timeout                                         │
  └────────────────┬──────────────────────────────────────────┬───────────────┘
                   ▼                                          ▼
  ┌────────────────────────────────┐          ┌───────────────────────────────┐
  │ accept path: one per ring      │          │ Connection: one per conn      │
  │  submit_accept(idx)            │          │  _fd / _id / peer ip:port     │
  │   state Running? at the limit? │          │  _ipv6 flag                   │
  │   CAS _accept_inflight[idx]    │          │  _in_buffer   kernel fills it │
  │   async_accept(listen_fd)      │          │  _out_buffer  in flight       │
  │   (single shot; multishot      │          │  _pending_buffer  new data    │
  │    only for one thread)        │          │  _send_mtx / _sending         │
  │  on_accept_complete()          │          │  _max_pending_send (10 MB)    │
  │   TCP_NODELAY / keepalive      │          │  _state: Connectioning ->     │
  │   new Connection, re-check     │          │          Connected ->         │
  │   insert into _conns,          │          │          DisConnecting        │
  │   establish(), first recv      │          │  _timer_gen + _last_active    │
  │  EMFILE: rescue fd + 1 s       │          │  callbacks frozen by          │
  └────────────────────────────────┘          │  establish()                  │
                                              │  _context (any + rw lock)     │
                                              └───────────────────────────────┘
```
### 2.2.2 TcpServer Data Flow
```
client connects
   │
   ▼
[worker] CQE:ACCEPT(fd) ── on_accept_complete()
   ├─ state != Running ────────► ::close(fd), drop it
   ├─ at the connection limit ─► conn->discard() (fd only) + cancel accepts on all rings
   ├─ otherwise: Connection(uring, fd, id, pool) ─► insert into _conns under the
   │              same lock ─► establish()
   │                └─ on_connected ─► freeze callbacks ─► submit the first RECV
   └─ single-shot accept ─► submit_accept(idx) re-arms it
   │
   ▼
[worker] CQE:RECV(n) ── on_recv_complete()
   ├─ n <= 0 ──────────────────► shutdown()
   ├─ refresh_timeout() (writes a timestamp only)
   ├─ user on_recv(conn, in_buffer)
   │    ├─ conn->send(...) ──► append to _out_buffer ─► submit SEND
   │    └─ conn->consume(n) ─► advance the _in_buffer read cursor
   ├─ filled the buffer below 64 KB ─► reserve(2x); mostly idle ─► shrink()
   └─ submit the next RECV (pipelined)
   │
   ▼
[worker] CQE:SEND(n) ── on_send_complete()
   ├─ n <= 0 ──────────────────► abort_close()
   ├─ merge _pending_buffer into _out_buffer
   ├─ more data ───────────────► submit SEND again
   └─ drained ─────────────────► clear _sending ─► fire the send-drained callback
   │
   ▼
peer closes/error/idle timeout/explicit shutdown
   └─ shutdown(): flush queued data ─► close_now(): ::shutdown(SHUT_RDWR) wakes a
      pending recv, then async_close
        └─ on_close_complete() ─► on_close ─► clear callback (remove from _conns,
           wake drain waiters)
```

## 2.3 UdpServer
### 2.3.1 UdpServer Architecture Diagram
```
                        Socket layer/rings/per-ring state
                                       │
                                       ▼
  ┌────────────────────────────────────────────────────────────────────────┐
  │ _socket             primary socket (port private by default)           │
  │ _extra_sockets      multi-socket mode: one socket per ring             │
  │ _urings             one IoUring + worker thread per slot               │
  │ _recv_multishot     off by default; kernel 6.0+ for path B             │
  │ _recv_depth / _recv_buffer_size / _recv_bufring_entries                │
  │ per ring            _recv_free / _send_free pools + in-flight counters │
  │ _recvmsg_inflight   at most one multishot in flight per ring           │
  └──────────────┬─────────────────────────────────────────┬───────────────┘
                 ▼   receive                               ▼   send
  ┌────────────────────────────┐          ┌────────────────────────────────┐
  │ path A (default)           │          │ sendto(data, len, dest, uring) │
  │  _recv_depth (4) recvmsg   │          │  ring: explicit uring >        │
  │  requests in flight per    │          │  current worker's ring >       │
  │  ring, each owning one     │          │  _next_uring round-robin       │
  │  buffer; the completion    │          │  acquire_send_buffer(idx)      │
  │  returns it and            │          │   pool buffer + in-flight      │
  │  refill_recv(idx) tops     │          │   registration, one lock       │
  │  the depth back up         │          │  async_sendmsg(fd, buf, dest)  │
  └────────────────────────────┘          │  res < 0 reported in the       │
                                          │  completion; the buffer is     │
  ┌────────────────────────────┐          │  returned either way           │
  │ path B (opt-in, instead    │          │  cap: 4096 in flight / 8 MB    │
  │ of path A)                 │          └────────────────────────────────┘
  │  multishot + buf ring      │
  │  one recvmsg per ring:     │
  │  IORING_RECV_MULTISHOT     │
  │  + IOSQE_BUFFER_SELECT     │
  │  CQE(F_MORE, bid)          │
  │   -> on_recv_view(payload) │
  │  payload valid only in     │
  │  the callback              │
  │  recycle_recv_buffer(bid)  │
  │  no F_MORE -> re-arm       │
  └────────────────────────────┘
```
### 2.3.2 UdpServer Data Flow
```
[worker] path A: CQE:RECVMSG(n)
   ├─ take the buffer owned by this request
   ├─ res < 0 ─► return the buffer to the pool; after 3 consecutive failures
   │             back off for 1 s (EBADF/ENOBUFS must not spin)
   └─ res > 0 ─► parse the source address (v4/v6) ─► on_recv(buf, src, uring)
                 or on_recv_v6(...)
              ─► return the buffer ─► refill_recv(idx)

[worker] path B: CQE:RECVMSG(F_MORE, bid)
   ├─ no F_BUFFER flag ─► count and drop (bid is invalid here; returning it would
   │                      duplicate entries in the ring)
   ├─ io_uring_recvmsg_validate ─► io_uring_recvmsg_name/payload
   ├─ MSG_TRUNC ─► truncated-datagram counter
   ├─ on_recv_view(payload, len, src, uring)   ← zero copy, valid only in the callback
   ├─ recycle_recv_buffer(bid)                 ← unconditional
   └─ no F_MORE ─► clear the in-flight flag ─► re-arm (back off 1 s on failure)

business thread/worker
   ├─ state != Running ─► count the rejection and drop
   ├─ pick a ring ─► acquire_send_buffer() (pool buffer + in-flight registration)
   ├─ async_sendmsg(fd, buf, dest) ─► CQE: res < 0 reported; buffer returned either way
   └─ over the in-flight cap ─► drop + send_backpressure_drops + rate-limited WARN
```
## 2.4 Log

### 2.4.1 Architecture Diagram

```
    business threads(any thread, including a worker running a callback)
                                    │
                                    ▼
  ┌────────────────────────────────────────────────────────────────────────┐
  │ TRACE/DEBUG/INFO/WARN/ERROR/FATAL macros                               │
  │  log(level, source_location, fmt, args...)                             │
  │   1. level filter -> to_console / to_file (both false: return here)    │
  │   2. capture metadata on this thread: clock_gettime, gettid, tag tN    │
  │   3. move every argument into the Record by value (const char*,        │
  │      char[], string_view are copied into std::string)                  │
  │   4. body = [fmt, args...]: std::format runs later, on the backend     │
  └────────────────────────────────────────────────────────────────────────┘
                                    │ Record (by value)
                                    ▼
  ┌────────────────────────────────────────────────────────────────────────┐
  │ _front: producers append one Record each, no formatting under the lock │
  │  full (LOG_RECORDS_PER_BUF = 512) -> hand the buffer over              │
  │  ERROR or above -> hand over and wait until that batch is on disk      │
  │  otherwise the 200 ms interval (LOG_FLUSH_INTERVAL_MS) lets the        │
  │  backend take the half-full buffer as it is                            │
  │  handoff: _front <-> _back under _buf_mtx; at most two buffers exist   │
  │  a producer that must hand over waits on _space_cv for the backend     │
  │  _handed / _done count handed-over and completed batches, so flush()   │
  │  and ERROR/FATAL wait for their own batch instead of for "idle"        │
  └────────────────────────────────────────────────────────────────────────┘
                                    │ swap
                                    ▼
  ┌────────────────────────────────────────────────────────────────────────┐
  │ backend thread (std::jthread) -> write_batch()                         │
  │  std::vformat_to into one reused std::string body                      │
  │  assemble [time][LEVEL][tN][file:line]: body in char[LOG_BUF_SIZE]     │
  │  format throws -> "<log format error: ...>", format_errors++           │
  │  console: colour only the level bracket; FATAL flushes stdout          │
  │  file: append to _out, one write() per LOG_WRITE_BLOCK = 64 KB         │
  │  _done = _handed, then notify _space_cv and _drained_cv                │
  └─────────────┬────────────────────────────────────────────┬─────────────┘
                ▼                                            ▼
  ┌──────────────────────────┐                ┌────────────────────────────┐
  │ console (stdout)         │                │ log file: YYYY_MM_DD_N.log │
  │  · optional ANSI colour, │                │  · opened lazily, never    │
  │    level bracket only    │                │    pre-created             │
  │  · _console_level gate   │                │  · O_APPEND + O_NOFOLLOW   │
  │  · FATAL forces fflush   │                │    + O_EXCL, mode 0644     │
  └──────────────────────────┘                │  · rotate at LOG_MAX_LINES │
                                              │    (10240) lines           │
                                              │  · index continues after   │
                                              │    scan_max_index()        │
                                              └────────────────────────────┘
```