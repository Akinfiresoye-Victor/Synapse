# ═══════════════════════════════════════════════════════════════════════════════
#  client.py  —  Synapse  v1.0
#
#  Features:
#  ─────────
#  • TCP_NODELAY + large socket buffers  →  same low-lag tuning as the server
#  • Auto-reconnect with exponential back-off  →  survives brief network hiccups
#  • Real-time progress bars (no external libs)  →  watch every byte fly
#  • Batch upload (SEND_MULTI)  →  drop a whole folder to the server at once
#  • Browse server files before downloading (LIST_FILES)
#  • Group chat (CHAT)  →  see messages from all connected clients
#  • Ping / latency check
#  • Server health report (SERVER_INFO)
#  • Set a display name so chat is readable
#  • Colourised terminal output using plain ANSI codes  →  zero dependencies
# ═══════════════════════════════════════════════════════════════════════════════

import socket
import hashlib
import os
import sys
import time
import threading
from datetime import datetime

# ══════════════════════════════════════════════════════════════════════════════
#  ANSI COLOUR PALETTE  (works on every Linux / macOS terminal; on Windows
#  enable it by running  `chcp 65001` once in CMD, or just use Windows Terminal)
# ══════════════════════════════════════════════════════════════════════════════

class C:
    RESET  = "\033[0m"
    BOLD   = "\033[1m"
    DIM    = "\033[2m"
    RED    = "\033[91m"
    GREEN  = "\033[92m"
    YELLOW = "\033[93m"
    BLUE   = "\033[94m"
    MAGENTA= "\033[95m"
    CYAN   = "\033[96m"
    WHITE  = "\033[97m"


# ─── Convenience printers ──────────────────────────────────────────────────────

def _p(color: str, icon: str, msg: str) -> None:
    print(f"{color}{icon}  {msg}{C.RESET}")

def ok(msg: str)   : _p(C.GREEN,   "  ✓", msg)
def err(msg: str)  : _p(C.RED,     "  ✗", msg)
def info(msg: str) : _p(C.CYAN,    "  ℹ", msg)
def warn(msg: str) : _p(C.YELLOW,  "  ⚠", msg)

def banner(title: str) -> None:
    w = 58
    border = f"{C.BOLD}{C.BLUE}{'═' * w}{C.RESET}"
    print(f"\n{border}")
    print(f"{C.BOLD}{C.WHITE}   {title}{C.RESET}")
    print(border)


# ══════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION  — change SERVER_IP to the machine running server.py
# ══════════════════════════════════════════════════════════════════════════════

DEFAULT_IP   = "127.0.0.1"
DEFAULT_PORT = 1234
ENCODER      = "utf-8"
BYTESIZE     = 131072       # 128 KB — matches server chunk size
DOWNLOAD_DIR = "client_downloads"

MAX_RETRIES  = 4            # how many reconnect attempts before giving up
RETRY_BASE   = 1.5          # base for exponential back-off (seconds)

os.makedirs(DOWNLOAD_DIR, exist_ok=True)


# ══════════════════════════════════════════════════════════════════════════════
#  LENGTH-PREFIXED PROTOCOL  (must mirror server.py exactly)
# ══════════════════════════════════════════════════════════════════════════════

def _recv_n(sock: socket.socket, n: int) -> bytes:
    """Read exactly n bytes from the socket — blocks until done or error."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(min(n - len(buf), BYTESIZE))
        if not chunk:
            raise ConnectionError("Server closed the connection")
        buf += chunk
    return bytes(buf)


def recv_msg(sock: socket.socket) -> str:
    """Receive one complete length-prefixed message as a string."""
    length = int.from_bytes(_recv_n(sock, 4), "big")
    return _recv_n(sock, length).decode(ENCODER)


def send_msg(sock: socket.socket, text: str) -> None:
    """Send one complete length-prefixed message."""
    data = text.encode(ENCODER)
    sock.sendall(len(data).to_bytes(4, "big") + data)


# ─── Utility ──────────────────────────────────────────────────────────────────

def sha256_file(path: str) -> str:
    """Compute the SHA-256 hex digest of a local file, reading in chunks."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(BYTESIZE), b""):
            h.update(chunk)
    return h.hexdigest()


def fmt_size(n: int | float) -> str:
    """Convert bytes to a human-readable string, e.g. 2.3 MB."""
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def progress_bar(done: int, total: int, speed: float, width: int = 36) -> str:
    """
    Build a single-line progress bar string.

    done  — bytes transferred so far
    total — total bytes expected
    speed — current transfer speed in MB/s
    width — number of characters for the bar itself
    """
    pct  = done / total if total else 1.0
    fill = int(width * pct)
    bar  = f"{'█' * fill}{'░' * (width - fill)}"
    return (
        f"{C.CYAN}[{bar}]{C.RESET} "
        f"{C.BOLD}{pct*100:5.1f}%{C.RESET}  "
        f"{fmt_size(done):>10}/{fmt_size(total):<10}  "
        f"{C.YELLOW}{speed:6.2f} MB/s{C.RESET}"
    )


def print_progress(done: int, total: int, t0: float) -> None:
    """Overwrite the current terminal line with the latest progress."""
    elapsed = max(time.perf_counter() - t0, 1e-9)
    speed   = done / elapsed / 1_048_576
    print(f"\r  {progress_bar(done, total, speed)}", end="", flush=True)


def unique_local_path(filename: str) -> str:
    """
    Return a path inside DOWNLOAD_DIR for filename.
    Appends a timestamp if a file with that name already exists locally.
    """
    dest = os.path.join(DOWNLOAD_DIR, os.path.basename(filename))
    if not os.path.exists(dest):
        return dest
    name, ext = os.path.splitext(os.path.basename(filename))
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(DOWNLOAD_DIR, f"{name}_{ts}{ext}")


# ══════════════════════════════════════════════════════════════════════════════
#  CONNECTION  (with auto-reconnect and exponential back-off)
# ══════════════════════════════════════════════════════════════════════════════

_server_ip   = DEFAULT_IP
_server_port = DEFAULT_PORT


def _make_socket() -> socket.socket:
    """
    Create a TCP socket with the same low-latency tuning as the server.

    TCP_NODELAY explained simply:
      Normally TCP collects small outgoing data into one big packet to save
      bandwidth — this is called Nagle's algorithm.  The problem: if your app
      sends a short command ("PING") and then waits for a reply, TCP may hold
      that command in a buffer for ~200 ms before actually sending it.  That is
      where the 'lag' comes from.  Setting TCP_NODELAY = 1 tells TCP: "send
      IMMEDIATELY, even if the packet is tiny."

    SO_SNDBUF / SO_RCVBUF:
      The OS keeps an internal queue for outgoing and incoming data.  Bumping
      these to 1 MB lets large files move in bigger bursts without pausing to
      wait for the kernel to drain the queue.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY,  1)
    s.setsockopt(socket.SOL_SOCKET,  socket.SO_SNDBUF,    1 << 20)
    s.setsockopt(socket.SOL_SOCKET,  socket.SO_RCVBUF,    1 << 20)
    s.setsockopt(socket.SOL_SOCKET,  socket.SO_KEEPALIVE, 1)
    return s


def connect(ip: str, port: int, silent: bool = False) -> socket.socket | None:
    """
    Try to connect up to MAX_RETRIES times with exponential back-off.

    Exponential back-off means: wait 1.5 s, then 2.25 s, then 3.4 s, etc.
    This avoids hammering a server that is still starting up.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if not silent:
                info(f"Connecting to {ip}:{port}  (attempt {attempt}/{MAX_RETRIES})…")
            s = _make_socket()
            s.settimeout(8)
            s.connect((ip, port))
            s.settimeout(None)          # switch back to blocking mode

            welcome = recv_msg(s)
            parts   = welcome.split("|")
            if parts[0] == "WELCOME":
                ok(f"Connected to  {parts[1]}  at  {parts[2]}")
                ok(f"Clients online: {parts[3]}")
            return s
        except (ConnectionRefusedError, socket.timeout):
            warn(f"Attempt {attempt} failed — server not reachable")
        except Exception as e:
            warn(f"Attempt {attempt} error: {e}")

        if attempt < MAX_RETRIES:
            delay = RETRY_BASE ** attempt
            info(f"Retrying in {delay:.1f}s…")
            time.sleep(delay)

    err("Could not connect after all attempts.")
    return None


def reconnect(ip: str, port: int) -> socket.socket | None:
    """
    Called automatically when a command loses its connection mid-session.
    Tries to reconnect silently with back-off.
    """
    warn("Connection lost — attempting reconnect…")
    return connect(ip, port, silent=False)


# ══════════════════════════════════════════════════════════════════════════════
#  COMMAND IMPLEMENTATIONS
# ══════════════════════════════════════════════════════════════════════════════

# ─── SEND FILE (single) ───────────────────────────────────────────────────────

def _upload_one(sock: socket.socket, path: str, label: str = "") -> bool:
    """
    Upload a single file. Reusable by both send_file and send_multi.
    Returns True on success.
    """
    filename = os.path.basename(path)
    filesize = os.path.getsize(path)

    info(f"Hashing  {label or filename}  ({fmt_size(filesize)})…")
    fhash = sha256_file(path)
    ok(f"SHA-256: {fhash[:20]}…")

    send_msg(sock, filename)
    send_msg(sock, str(filesize))
    send_msg(sock, fhash)

    info(f"Uploading  {label or filename}…")
    sent  = 0
    t0    = time.perf_counter()
    last  = t0

    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(BYTESIZE), b""):
            sock.sendall(chunk)
            sent += len(chunk)
            now   = time.perf_counter()
            if now - last >= 0.15:          # refresh bar ~6 times per second
                print_progress(sent, filesize, t0)
                last = now

    print_progress(filesize, filesize, t0)
    print()

    result = recv_msg(sock)
    parts  = result.split("|")
    if parts[0] == "OK":
        speed = float(parts[2]) if len(parts) > 2 else 0.0
        ok(f"Saved as '{parts[1]}'  @ {speed:.2f} MB/s")
        return True
    else:
        err(f"Upload failed: {'|'.join(parts[1:])}")
        return False


def cmd_send_file(sock: socket.socket) -> None:
    """Upload a single file to the server."""
    banner("Upload File")
    path = input(f"{C.CYAN}  File path: {C.RESET}").strip().strip('"')

    if not path:
        warn("Cancelled.")
        return
    if not os.path.isfile(path):
        err(f"Not found: {path}")
        return

    send_msg(sock, "SEND_FILE")
    if recv_msg(sock) != "READY":
        err("Server not ready for upload.")
        return

    _upload_one(sock, path)


# ─── SEND MULTI (batch upload) ────────────────────────────────────────────────

def cmd_send_multi(sock: socket.socket) -> None:
    """
    Upload multiple files in one go.
    You can give individual paths separated by commas, or a folder path.
    """
    banner("Batch Upload")
    raw = input(f"{C.CYAN}  File paths (comma-separated) or folder: {C.RESET}").strip().strip('"')

    if not raw:
        warn("Cancelled.")
        return

    # Gather all files to upload
    paths: list[str] = []
    for entry in raw.split(","):
        entry = entry.strip().strip('"')
        if os.path.isfile(entry):
            paths.append(entry)
        elif os.path.isdir(entry):
            for f in sorted(os.listdir(entry)):
                full = os.path.join(entry, f)
                if os.path.isfile(full):
                    paths.append(full)
        else:
            warn(f"Skipping (not found): {entry}")

    if not paths:
        err("No valid files found.")
        return

    info(f"{len(paths)} file(s) queued for upload.")
    send_msg(sock, "SEND_MULTI")
    if recv_msg(sock) != "READY":
        err("Server not ready for batch upload.")
        return

    send_msg(sock, str(len(paths)))

    n_ok = n_fail = 0
    for i, path in enumerate(paths, 1):
        print(f"\n{C.BOLD}{C.MAGENTA}  [{i}/{len(paths)}] {os.path.basename(path)}{C.RESET}")
        success = _upload_one(sock, path, label=f"[{i}/{len(paths)}]")
        if success:
            n_ok += 1
        else:
            n_fail += 1

    multi_done = recv_msg(sock)   # "MULTI_DONE|n_ok|n_fail"
    print()
    ok(f"Batch complete: {n_ok} succeeded, {n_fail} failed")


# ─── LIST SERVER FILES ────────────────────────────────────────────────────────

def cmd_list_server_files(sock: socket.socket) -> None:
    """Ask the server for its file list (with sizes) without downloading anything."""
    banner("Files on Server")
    send_msg(sock, "LIST_FILES")
    resp = recv_msg(sock)

    if resp == "NOFILES":
        warn("Server has no files stored.")
        return

    parts = resp.split("|")
    if parts[0] != "FILELIST":
        err(f"Unexpected response: {resp}")
        return

    entries = parts[1:]
    print(f"\n  {'#':>3}   {'Filename':<40}  {'Size':>10}")
    print(f"  {'─'*3}   {'─'*40}  {'─'*10}")
    for i, entry in enumerate(entries, 1):
        name, size_str = entry.rsplit(":", 1)
        print(f"  {C.YELLOW}{i:>3}{C.RESET}   {name:<40}  {C.DIM}{fmt_size(int(size_str)):>10}{C.RESET}")
    print()


# ─── GET FILE (download) ──────────────────────────────────────────────────────

def cmd_get_file(sock: socket.socket) -> None:
    """Download a file from the server with real-time progress and SHA-256 check."""
    banner("Download File")
    send_msg(sock, "GET_FILE")
    resp = recv_msg(sock)

    if resp == "NOFILES":
        warn("No files on server to download.")
        return

    parts = resp.split("|")
    if parts[0] != "FILES":
        err(f"Unexpected: {resp}")
        return

    files = parts[1:]
    print(f"\n  {'#':>3}   Filename")
    print(f"  {'─'*3}   {'─'*40}")
    for i, f in enumerate(files, 1):
        print(f"  {C.YELLOW}{i:>3}{C.RESET}   {f}")

    choice = input(f"\n{C.CYAN}  File number (Enter to cancel): {C.RESET}").strip()
    if not choice:
        send_msg(sock, "CANCEL")
        warn("Cancelled.")
        return

    send_msg(sock, choice)
    meta = recv_msg(sock)

    if meta.startswith("ERROR"):
        err(meta.split("|", 1)[1])
        return

    mparts   = meta.split("|")
    fname    = mparts[1]
    filesize = int(mparts[2])
    expected = mparts[3]

    info(f"File     : {fname}")
    info(f"Size     : {fmt_size(filesize)}")
    info(f"SHA-256  : {expected[:20]}…")

    dest = unique_local_path(fname)
    send_msg(sock, "ACK")

    info("Downloading…")
    received = 0
    hasher   = hashlib.sha256()
    t0       = time.perf_counter()
    last     = t0

    try:
        with open(dest, "wb") as f:
            while received < filesize:
                chunk = sock.recv(min(BYTESIZE, filesize - received))
                if not chunk:
                    raise ConnectionError("Server closed connection mid-download")
                f.write(chunk)
                hasher.update(chunk)
                received += len(chunk)
                now = time.perf_counter()
                if now - last >= 0.15:
                    print_progress(received, filesize, t0)
                    last = now
        print_progress(filesize, filesize, t0)
        print()
    except Exception as e:
        print()
        if os.path.exists(dest):
            os.remove(dest)
        err(f"Download failed: {e}")
        return

    if hasher.hexdigest() == expected and received == filesize:
        elapsed = max(time.perf_counter() - t0, 1e-9)
        speed   = received / elapsed / 1_048_576
        ok(f"Downloaded '{os.path.basename(dest)}'  @ {speed:.2f} MB/s")
        ok(f"Saved to:   {os.path.abspath(dest)}")
    else:
        os.remove(dest)
        err("Hash mismatch — corrupted file discarded. Try again.")


# ─── CHAT ─────────────────────────────────────────────────────────────────────

def cmd_chat(sock: socket.socket) -> None:
    """
    Join the group chat.
    Messages are broadcast to all connected clients by the server.
    A background thread listens for incoming messages so you can type and
    receive at the same time without blocking each other.
    """
    banner("Group Chat")
    send_msg(sock, "CHAT")
    resp = recv_msg(sock)
    if resp != "CHAT_START":
        err(f"Unexpected: {resp}")
        return

    info("Chat started.  Type messages and press Enter.  Type 'quit' to leave.\n")

    stop_flag = threading.Event()

    def _listener():
        """Background thread: print messages arriving from the server."""
        while not stop_flag.is_set():
            try:
                msg = recv_msg(sock)
                if msg == "CHAT_END":
                    stop_flag.set()
                    break
                # Print on its own line, then reprint the input prompt below it
                print(f"\r{C.GREEN}  {msg}{C.RESET}\n{C.CYAN}  You: {C.RESET}", end="", flush=True)
            except Exception:
                stop_flag.set()
                break

    listener_thread = threading.Thread(target=_listener, daemon=True)
    listener_thread.start()

    while not stop_flag.is_set():
        try:
            msg = input(f"{C.CYAN}  You: {C.RESET}").strip()
        except (EOFError, KeyboardInterrupt):
            msg = "quit"

        if msg.lower() in ("quit", "exit", "q", ""):
            send_msg(sock, "CHAT_QUIT")
            stop_flag.set()
            break

        send_msg(sock, msg)

    listener_thread.join(timeout=2)
    ok("Left the chat.")


# ─── PING ─────────────────────────────────────────────────────────────────────

def cmd_ping(sock: socket.socket) -> None:
    """Measure round-trip time to the server."""
    banner("Ping")
    samples = []
    for i in range(5):
        t0 = time.perf_counter()
        send_msg(sock, "PING")
        resp = recv_msg(sock)
        rtt  = (time.perf_counter() - t0) * 1000
        if resp == "PONG":
            samples.append(rtt)
            status = f"{C.GREEN}pong{C.RESET}"
        else:
            status = f"{C.RED}??{C.RESET}"
        print(f"  [{i+1}] {status}   RTT = {rtt:.2f} ms")
        time.sleep(0.1)

    if samples:
        avg = sum(samples) / len(samples)
        mn  = min(samples)
        mx  = max(samples)
        print(f"\n  {C.BOLD}avg {avg:.2f} ms   min {mn:.2f} ms   max {mx:.2f} ms{C.RESET}")


# ─── SERVER INFO ──────────────────────────────────────────────────────────────

def cmd_server_info(sock: socket.socket) -> None:
    """Display server health: uptime, clients, file count, disk space."""
    banner("Server Info")
    send_msg(sock, "SERVER_INFO")
    resp = recv_msg(sock)
    parts = resp.split("|")
    if parts[0] != "INFO":
        err(f"Unexpected: {resp}")
        return

    uptime, clients, files, disk_mb = parts[1], parts[2], parts[3], parts[4]
    print(f"\n  Uptime       :  {C.CYAN}{uptime}{C.RESET}")
    print(f"  Clients      :  {C.CYAN}{clients}{C.RESET}")
    print(f"  Stored files :  {C.CYAN}{files}{C.RESET}")
    print(f"  Disk free    :  {C.CYAN}{float(disk_mb):.0f} MB{C.RESET}\n")


# ─── SET NAME ─────────────────────────────────────────────────────────────────

def cmd_set_name(sock: socket.socket) -> None:
    """Set your display name for the group chat."""
    name = input(f"{C.CYAN}  Your chat name (max 32 chars): {C.RESET}").strip()
    if not name:
        warn("Cancelled.")
        return
    send_msg(sock, f"SET_NAME|{name[:32]}")
    resp = recv_msg(sock)
    parts = resp.split("|")
    if parts[0] == "NAME_OK":
        ok(f"Name set to '{parts[1]}'")
    else:
        err(f"Unexpected: {resp}")


# ─── LOCAL FILES ──────────────────────────────────────────────────────────────

def cmd_list_local() -> None:
    """Show all files you have downloaded so far."""
    banner("My Downloaded Files")
    files = sorted(
        f for f in os.listdir(DOWNLOAD_DIR)
        if os.path.isfile(os.path.join(DOWNLOAD_DIR, f))
    )
    if not files:
        warn(f"No files in {DOWNLOAD_DIR}/")
        return
    for f in files:
        size = os.path.getsize(os.path.join(DOWNLOAD_DIR, f))
        print(f"  {C.YELLOW}•{C.RESET}  {f:<44}  {C.DIM}{fmt_size(size):>10}{C.RESET}")
    print()


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN MENU
# ══════════════════════════════════════════════════════════════════════════════

MENU = f"""
{C.BOLD}{C.BLUE}  ╔══════════════════════════════════════════════╗{C.RESET}
{C.BOLD}{C.BLUE}  ║   Synapse  ·  Client  v1.0            ║{C.RESET}
{C.BOLD}{C.BLUE}  ╠══════════════════════════════════════════════╣{C.RESET}
{C.BOLD}{C.BLUE}  ║{C.RESET}  {C.YELLOW}1{C.RESET}  Upload a file                         {C.BOLD}{C.BLUE}║{C.RESET}
{C.BOLD}{C.BLUE}  ║{C.RESET}  {C.YELLOW}2{C.RESET}  Upload multiple files / folder         {C.BOLD}{C.BLUE}║{C.RESET}
{C.BOLD}{C.BLUE}  ║{C.RESET}  {C.YELLOW}3{C.RESET}  Browse server files                    {C.BOLD}{C.BLUE}║{C.RESET}
{C.BOLD}{C.BLUE}  ║{C.RESET}  {C.YELLOW}4{C.RESET}  Download a file                        {C.BOLD}{C.BLUE}║{C.RESET}
{C.BOLD}{C.BLUE}  ║{C.RESET}  {C.YELLOW}5{C.RESET}  Group chat                             {C.BOLD}{C.BLUE}║{C.RESET}
{C.BOLD}{C.BLUE}  ║{C.RESET}  {C.YELLOW}6{C.RESET}  Ping server                            {C.BOLD}{C.BLUE}║{C.RESET}
{C.BOLD}{C.BLUE}  ║{C.RESET}  {C.YELLOW}7{C.RESET}  Server health info                     {C.BOLD}{C.BLUE}║{C.RESET}
{C.BOLD}{C.BLUE}  ║{C.RESET}  {C.YELLOW}8{C.RESET}  Set my chat name                       {C.BOLD}{C.BLUE}║{C.RESET}
{C.BOLD}{C.BLUE}  ║{C.RESET}  {C.YELLOW}9{C.RESET}  My downloaded files                    {C.BOLD}{C.BLUE}║{C.RESET}
{C.BOLD}{C.BLUE}  ║{C.RESET}  {C.YELLOW}q{C.RESET}  Quit                                   {C.BOLD}{C.BLUE}║{C.RESET}
{C.BOLD}{C.BLUE}  ╚══════════════════════════════════════════════╝{C.RESET}"""


def main() -> None:
    global _server_ip, _server_port

    # ── Startup banner ──────────────────────────────────────────────────────
    print(f"\n{C.BOLD}{C.CYAN}  ╔══════════════════════════════╗")
    print(f"  ║   Synapse  v1.0       ║")
    print(f"  ╚══════════════════════════════╝{C.RESET}\n")

    # ── Server address ──────────────────────────────────────────────────────
    ip_in   = input(f"{C.CYAN}  Server IP    [{DEFAULT_IP}]:   {C.RESET}").strip()
    port_in = input(f"{C.CYAN}  Server Port  [{DEFAULT_PORT}]:      {C.RESET}").strip()

    _server_ip   = ip_in   if ip_in   else DEFAULT_IP
    _server_port = int(port_in) if port_in.isdigit() else DEFAULT_PORT

    sock = connect(_server_ip, _server_port)
    if not sock:
        sys.exit(1)

    # ── Main loop ────────────────────────────────────────────────────────────
    while True:
        print(MENU)
        try:
            choice = input(f"\n{C.CYAN}  › {C.RESET}").strip().lower()
        except (EOFError, KeyboardInterrupt):
            choice = "q"

        # Every command is wrapped in error recovery so one crash doesn't exit
        try:
            if   choice == "1": cmd_send_file(sock)
            elif choice == "2": cmd_send_multi(sock)
            elif choice == "3": cmd_list_server_files(sock)
            elif choice == "4": cmd_get_file(sock)
            elif choice == "5": cmd_chat(sock)
            elif choice == "6": cmd_ping(sock)
            elif choice == "7": cmd_server_info(sock)
            elif choice == "8": cmd_set_name(sock)
            elif choice == "9": cmd_list_local()
            elif choice == "q":
                try:
                    send_msg(sock, "QUIT")
                    recv_msg(sock)   # receive "BYE"
                except Exception:
                    pass
                ok("Disconnected. Goodbye!")
                break
            elif choice == "":
                pass               # just re-show the menu
            else:
                warn(f"Unknown option '{choice}'")

        except ConnectionError as e:
            err(f"Connection lost: {e}")
            sock_new = reconnect(_server_ip, _server_port)
            if sock_new:
                sock = sock_new
                ok("Reconnected! Continuing session.")
            else:
                err("Could not reconnect. Exiting.")
                break
        except Exception as e:
            err(f"Unexpected error: {e}")
            # Don't exit — let user try again

    try:
        sock.close()
    except Exception:
        pass


if __name__ == "__main__":
    main()