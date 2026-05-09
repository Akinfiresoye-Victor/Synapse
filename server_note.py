# ═══════════════════════════════════════════════════════════════════════════════
#  server.py  —  Synapse  v1.0
#
#  What changed from v3:
#  ─────────────────────
#  • TCP_NODELAY on every accepted socket  →  kills perceived lag instantly
#  • SO_SNDBUF / SO_RCVBUF bumped to 1 MB →  fewer kernel round-trips on big files
#  • SO_KEEPALIVE on every accepted socket →  dead clients detected automatically
#  • Broadcast chat  →  all connected clients receive each other's messages
#  • LIST_FILES command  →  client can query available files without downloading
#  • SERVER_INFO command →  returns uptime, client count, disk usage
#  • SEND_MULTI command  →  client can upload multiple files in one session
#  • Graceful Ctrl+C now closes ALL client sockets cleanly
#  • Structured shutdown event replaces bare KeyboardInterrupt hacks
# ═══════════════════════════════════════════════════════════════════════════════

import socket
import threading
import hashlib
import os
import logging
import time
import shutil
from datetime import datetime, timedelta

# ─── Logging setup ────────────────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/server.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ─── Configuration ────────────────────────────────────────────────────────────
HOST_IP      = socket.gethostbyname(socket.gethostname())
HOST_PORT    = 1234
ENCODER      = "utf-8"
BYTESIZE     = 131072       # 128 KB chunks — bigger = faster bulk transfers
UPLOAD_DIR   = "server_files"
IDLE_TIMEOUT = 300          # drop idle client after 5 minutes

os.makedirs(UPLOAD_DIR, exist_ok=True)

# ─── Server-wide state ────────────────────────────────────────────────────────
_clients: dict        = {}             # addr_str → {"sock", "addr", "since", "name"}
_clients_lock         = threading.Lock()
_shutdown_event       = threading.Event()
_server_start         = datetime.now()

# ─── Broadcast helper ─────────────────────────────────────────────────────────

def broadcast(sender_addr: str, message: str) -> None:
    """
    Send a chat message to EVERY connected client except the sender.
    We hold the lock only while we copy the socket list so we don't
    block new connections during a slow send.
    """
    with _clients_lock:
        targets = [
            (addr, info["sock"])
            for addr, info in _clients.items()
            if addr != sender_addr
        ]
    for addr, sock in targets:
        try:
            send_msg(sock, message)
        except Exception:
            pass  # dead socket — its thread will clean up


# ══════════════════════════════════════════════════════════════════════════════
#  LENGTH-PREFIXED PROTOCOL
#  Every message = [4 bytes big-endian length] + [UTF-8 payload]
# ══════════════════════════════════════════════════════════════════════════════

def _recv_n(sock: socket.socket, n: int) -> bytes:
    """Pull exactly n bytes from the socket, blocking until done or error."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(min(n - len(buf), BYTESIZE))
        if not chunk:
            raise ConnectionError("Socket closed unexpectedly")
        buf += chunk
    return bytes(buf)


def recv_msg(sock: socket.socket) -> str:
    """Receive one complete length-prefixed message, returned as a string."""
    length = int.from_bytes(_recv_n(sock, 4), "big")
    return _recv_n(sock, length).decode(ENCODER)


def send_msg(sock: socket.socket, text: str) -> None:
    """
    Send one complete length-prefixed message.
    sendall() is critical here — it loops internally until every byte is
    written even if the kernel only accepts part of the data at once.
    """
    data = text.encode(ENCODER)
    sock.sendall(len(data).to_bytes(4, "big") + data)


# ─── File helpers ─────────────────────────────────────────────────────────────

def sha256_file(path: str) -> str:
    """SHA-256 digest of a file, reading in BYTESIZE chunks to keep RAM low."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(BYTESIZE), b""):
            h.update(chunk)
    return h.hexdigest()


def unique_path(directory: str, filename: str) -> str:
    """
    Returns a safe path inside directory for filename.
    If the filename already exists a timestamp suffix is appended so we
    NEVER silently overwrite an existing upload.
    """
    target = os.path.join(directory, os.path.basename(filename))
    if not os.path.exists(target):
        return target
    name, ext = os.path.splitext(os.path.basename(filename))
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(directory, f"{name}_{ts}{ext}")


def list_files() -> list:
    """Sorted list of files currently stored in the upload directory."""
    return sorted(
        f for f in os.listdir(UPLOAD_DIR)
        if os.path.isfile(os.path.join(UPLOAD_DIR, f))
    )


def fmt_size(n: int) -> str:
    """Human-readable file size (e.g. 1.4 MB)."""
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


# ══════════════════════════════════════════════════════════════════════════════
#  COMMAND HANDLERS
# ══════════════════════════════════════════════════════════════════════════════

def h_send_file(sock: socket.socket, addr: str) -> None:
    """
    Handle a single file upload from the client.

    Flow:
      Server → "READY"
      Client → filename
      Client → filesize (str)
      Client → sha256   (hex)
      Client → [raw bytes × filesize]
      Server → "OK|saved_name|speed_MB_s"  or  "FAIL|reason"
    """
    send_msg(sock, "READY")

    filename      = recv_msg(sock)
    filesize      = int(recv_msg(sock))
    expected_hash = recv_msg(sock)

    dest = unique_path(UPLOAD_DIR, filename)
    saved_name = os.path.basename(dest)
    log.info(f"[{addr}] ↑ receiving '{saved_name}'  ({fmt_size(filesize)})")

    received = 0
    hasher   = hashlib.sha256()
    t0       = time.perf_counter()

    try:
        with open(dest, "wb") as f:
            while received < filesize:
                chunk = sock.recv(min(BYTESIZE, filesize - received))
                if not chunk:
                    raise ConnectionError("Client dropped mid-upload")
                f.write(chunk)
                hasher.update(chunk)
                received += len(chunk)
    except Exception as e:
        if os.path.exists(dest):
            os.remove(dest)
        send_msg(sock, f"FAIL|Transfer interrupted: {e}")
        log.warning(f"[{addr}] ✗ upload aborted: {e}")
        return

    elapsed = max(time.perf_counter() - t0, 1e-9)
    speed   = received / elapsed / 1_048_576  # bytes/s → MB/s

    if hasher.hexdigest() == expected_hash and received == filesize:
        send_msg(sock, f"OK|{saved_name}|{speed:.2f}")
        log.info(f"[{addr}] ✓ saved '{saved_name}'  ({speed:.2f} MB/s)")
    else:
        os.remove(dest)
        send_msg(sock, "FAIL|Hash mismatch — file discarded")
        log.warning(f"[{addr}] ✗ hash mismatch for '{filename}'")


def h_send_multi(sock: socket.socket, addr: str) -> None:
    """
    Handle a batch of file uploads in one session.

    Flow:
      Server → "READY"
      Client → count (str, number of files to send)
      For each file:
        [ same as h_send_file but skips the initial SEND_FILE/READY handshake ]
      Server → "MULTI_DONE|n_ok|n_fail"
    """
    send_msg(sock, "READY")
    count = int(recv_msg(sock))
    log.info(f"[{addr}] ↑↑ batch upload: {count} file(s)")

    n_ok = n_fail = 0
    for i in range(count):
        filename      = recv_msg(sock)
        filesize      = int(recv_msg(sock))
        expected_hash = recv_msg(sock)

        dest = unique_path(UPLOAD_DIR, filename)
        received = 0
        hasher   = hashlib.sha256()
        t0       = time.perf_counter()
        ok_flag  = True

        try:
            with open(dest, "wb") as f:
                while received < filesize:
                    chunk = sock.recv(min(BYTESIZE, filesize - received))
                    if not chunk:
                        raise ConnectionError("Client dropped mid-batch")
                    f.write(chunk)
                    hasher.update(chunk)
                    received += len(chunk)
        except Exception as e:
            if os.path.exists(dest):
                os.remove(dest)
            send_msg(sock, f"FAIL|{e}")
            ok_flag = False

        if ok_flag:
            elapsed = max(time.perf_counter() - t0, 1e-9)
            speed   = received / elapsed / 1_048_576
            if hasher.hexdigest() == expected_hash and received == filesize:
                send_msg(sock, f"OK|{os.path.basename(dest)}|{speed:.2f}")
                log.info(f"[{addr}] ✓ batch {i+1}/{count} '{os.path.basename(dest)}'  ({speed:.2f} MB/s)")
                n_ok += 1
            else:
                os.remove(dest)
                send_msg(sock, "FAIL|Hash mismatch")
                n_fail += 1

    send_msg(sock, f"MULTI_DONE|{n_ok}|{n_fail}")
    log.info(f"[{addr}] ↑↑ batch done: {n_ok} ok, {n_fail} failed")


def h_get_file(sock: socket.socket, addr: str) -> None:
    """
    Handle a file download request from the client.

    Flow:
      Server → "NOFILES"                  (nothing available)
      Server → "FILES|f1|f2|…"            (pipe-separated list)
      Client → choice (1-based int) | "CANCEL"
      Server → "ERROR|reason"             (bad index)
      Server → "META|name|size|sha256"
      Client → "ACK" | "NACK"
      Server → [raw bytes × size]         (only after ACK)
    """
    files = list_files()
    if not files:
        send_msg(sock, "NOFILES")
        return

    send_msg(sock, "FILES|" + "|".join(files))
    choice = recv_msg(sock)

    if choice == "CANCEL":
        return

    try:
        idx = int(choice) - 1
        assert 0 <= idx < len(files)
    except (ValueError, AssertionError):
        send_msg(sock, "ERROR|Invalid choice")
        return

    path     = os.path.join(UPLOAD_DIR, files[idx])
    filesize = os.path.getsize(path)
    fhash    = sha256_file(path)

    send_msg(sock, f"META|{files[idx]}|{filesize}|{fhash}")

    ack = recv_msg(sock)
    if ack != "ACK":
        log.info(f"[{addr}] download cancelled after META")
        return

    log.info(f"[{addr}] ↓ sending '{files[idx]}'  ({fmt_size(filesize)})")
    t0   = time.perf_counter()
    sent = 0

    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(BYTESIZE), b""):
            sock.sendall(chunk)
            sent += len(chunk)

    elapsed = max(time.perf_counter() - t0, 1e-9)
    speed   = sent / elapsed / 1_048_576
    log.info(f"[{addr}] ✓ sent '{files[idx]}'  ({speed:.2f} MB/s)")


def h_list_files(sock: socket.socket) -> None:
    """
    Return file list with sizes without triggering a download.

    Flow:
      Server → "NOFILES"
      Server → "FILELIST|name1:size1|name2:size2|…"
    """
    files = list_files()
    if not files:
        send_msg(sock, "NOFILES")
        return
    parts = []
    for f in files:
        sz = os.path.getsize(os.path.join(UPLOAD_DIR, f))
        parts.append(f"{f}:{sz}")
    send_msg(sock, "FILELIST|" + "|".join(parts))


def h_chat(sock: socket.socket, addr: str) -> None:
    """
    Broadcast chat — every message is echoed to ALL connected clients.

    The original server used input() which BLOCKED the entire thread.
    v3 fixed that with echo-only. v1 goes further: messages are broadcast
    to every other connected client so this works as a real group chat.

    Flow:
      Server → "CHAT_START"
      loop:
        Client → message  |  "CHAT_QUIT"
        Server → "[HH:MM:SS] <addr>: <message>"   (broadcast to all)
      Server → "CHAT_END"
    """
    with _clients_lock:
        display_name = _clients.get(addr, {}).get("name", addr)

    send_msg(sock, "CHAT_START")

    while True:
        try:
            msg = recv_msg(sock)
        except Exception:
            break

        if msg == "CHAT_QUIT":
            break

        ts        = datetime.now().strftime("%H:%M:%S")
        formatted = f"[{ts}] {display_name}: {msg}"
        log.info(f"[{addr}] 💬  {msg}")

        # Echo back to sender AND broadcast to everyone else
        send_msg(sock, formatted)
        broadcast(addr, formatted)

    send_msg(sock, "CHAT_END")


def h_server_info(sock: socket.socket) -> None:
    """
    Return a snapshot of server health.

    Response:
      "INFO|uptime_str|client_count|file_count|disk_free_MB"
    """
    uptime = str(timedelta(seconds=int((datetime.now() - _server_start).total_seconds())))

    with _clients_lock:
        n_clients = len(_clients)

    n_files   = len(list_files())
    free_bytes = shutil.disk_usage(UPLOAD_DIR).free
    free_mb    = free_bytes / 1_048_576

    send_msg(sock, f"INFO|{uptime}|{n_clients}|{n_files}|{free_mb:.0f}")


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN CLIENT HANDLER  (one daemon thread per connected client)
# ══════════════════════════════════════════════════════════════════════════════

def handle_client(sock: socket.socket, address: tuple) -> None:
    """
    Entry point for each client thread.

    We configure the socket here for low latency:
      • TCP_NODELAY  — disables Nagle's algorithm.
                       Nagle buffers small outgoing packets hoping to batch them.
                       For a command-response protocol this causes noticeable
                       delay because the server's "READY" reply may sit in the
                       buffer for ~200ms before flushing. TCP_NODELAY makes it
                       send immediately.
      • SO_SNDBUF/SO_RCVBUF — tells the kernel to allocate larger I/O buffers.
                       More buffer = fewer kernel interruptions during large
                       file transfers = better throughput.
      • SO_KEEPALIVE — the OS sends a heartbeat every ~2 hours by default to
                       detect dead peers so we don't hold zombie connections.
    """
    # ── Performance socket options ─────────────────────────────────────────
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY,  1)
    sock.setsockopt(socket.SOL_SOCKET,  socket.SO_SNDBUF,    1 << 20)  # 1 MB send buffer
    sock.setsockopt(socket.SOL_SOCKET,  socket.SO_RCVBUF,    1 << 20)  # 1 MB recv buffer
    sock.setsockopt(socket.SOL_SOCKET,  socket.SO_KEEPALIVE, 1)
    sock.settimeout(IDLE_TIMEOUT)

    addr = f"{address[0]}:{address[1]}"

    with _clients_lock:
        _clients[addr] = {
            "sock"  : sock,
            "addr"  : address,
            "since" : datetime.now(),
            "name"  : addr,          # can be updated by a SET_NAME command later
        }
        n = len(_clients)

    log.info(f"✚  {addr} connected   (clients online: {n})")

    try:
        send_msg(sock, f"WELCOME|Synapse v1|{HOST_IP}:{HOST_PORT}|{n}")

        while not _shutdown_event.is_set():
            try:
                cmd = recv_msg(sock)
            except socket.timeout:
                log.warning(f"[{addr}] idle for {IDLE_TIMEOUT}s — dropping")
                break

            if   cmd == "QUIT":
                send_msg(sock, "BYE")
                break
            elif cmd == "PING":
                send_msg(sock, "PONG")
            elif cmd == "SEND_FILE":
                h_send_file(sock, addr)
            elif cmd == "SEND_MULTI":
                h_send_multi(sock, addr)
            elif cmd == "GET_FILE":
                h_get_file(sock, addr)
            elif cmd == "LIST_FILES":
                h_list_files(sock)
            elif cmd == "CHAT":
                h_chat(sock, addr)
            elif cmd == "SERVER_INFO":
                h_server_info(sock)
            elif cmd.startswith("SET_NAME|"):
                new_name = cmd.split("|", 1)[1][:32].strip()
                with _clients_lock:
                    if addr in _clients:
                        _clients[addr]["name"] = new_name
                send_msg(sock, f"NAME_OK|{new_name}")
                log.info(f"[{addr}] renamed to '{new_name}'")
            else:
                send_msg(sock, f"ERROR|Unknown command '{cmd}'")

    except ConnectionError:
        log.info(f"[{addr}] connection dropped by client")
    except Exception as e:
        log.error(f"[{addr}] unhandled error: {e}", exc_info=True)
    finally:
        with _clients_lock:
            _clients.pop(addr, None)
        try:
            sock.close()
        except Exception:
            pass
        log.info(f"✖  {addr} disconnected  (clients online: {len(_clients)})")


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST_IP, HOST_PORT))
    srv.listen(50)

    log.info("=" * 56)
    log.info("   Synapse  Server  v1.0")
    log.info(f"   Listening  :  {HOST_IP}:{HOST_PORT}")
    log.info(f"   Files dir  :  {os.path.abspath(UPLOAD_DIR)}")
    log.info(f"   Chunk size :  {BYTESIZE // 1024} KB")
    log.info(f"   Idle limit :  {IDLE_TIMEOUT}s")
    log.info("=" * 56)

    try:
        while not _shutdown_event.is_set():
            try:
                srv.settimeout(1.0)          # wake up every second to check shutdown
                try:
                    cli_sock, cli_addr = srv.accept()
                except socket.timeout:
                    continue
                threading.Thread(
                    target=handle_client,
                    args=(cli_sock, cli_addr),
                    daemon=True,
                ).start()
            except OSError:
                break
    except KeyboardInterrupt:
        log.info("Ctrl+C — shutting down…")
        _shutdown_event.set()
    finally:
        # Close all active client sockets so their threads wake up and exit
        with _clients_lock:
            for info in _clients.values():
                try:
                    info["sock"].close()
                except Exception:
                    pass
        srv.close()
        log.info("Server stopped.")


if __name__ == "__main__":
    main()