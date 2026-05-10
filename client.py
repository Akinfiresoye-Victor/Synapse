# ═══════════════════════════════════════════════════════════════════════════════
#  client.py  —  Synapse  v2.0
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

# 'socket' gives us the ability to open a TCP connection to the server.
# Think of it like a phone — socket lets us "dial" the server's IP and port.
import socket

# 'hashlib' provides SHA-256, the algorithm we use to fingerprint files.
# If the fingerprint before and after sending is identical, the file arrived
# intact and nothing was corrupted in transit.
import hashlib

# 'os' lets us interact with the filesystem: check if a file exists,
# get its size, build file paths, list directory contents, and delete files.
import os

# 'sys' gives access to sys.exit() — used to quit the program cleanly
# when the connection completely fails on startup.
import sys

# 'time' is used for two things:
#   1. Measuring how long a transfer takes (so we can show MB/s speed)
#   2. Sleeping between reconnect attempts (back-off timer)
import time

# 'threading' lets us run two things at once.
# In chat mode we need to SEND and RECEIVE simultaneously — one thread handles
# each so neither blocks the other.
import threading

# 'datetime' is used to timestamp things (e.g. when a duplicate file is saved,
# we append YYYYMMDD_HHMMSS to the filename so nothing gets overwritten).
from datetime import datetime


# ══════════════════════════════════════════════════════════════════════════════
#  ANSI COLOUR PALETTE
#
#  ANSI escape codes are special character sequences that most terminals
#  understand as styling instructions rather than literal text.
#
#  Format:  \033[<code>m
#    \033  = the ESC character (octal 33, hex 1B)
#    [     = marks the start of the code sequence
#    <code>= a number that says what style to apply
#    m     = ends the sequence
#
#  Example:  "\033[92mHello\033[0m"
#             ↑ start green     ↑ reset to normal
#  Terminal renders: Hello   (in green, then colour resets)
#
#  Why a class?  We group all codes in one place so they are easy to find
#  and change.  Using C.GREEN instead of "\033[92m" everywhere makes the
#  code much more readable.
# ══════════════════════════════════════════════════════════════════════════════

class C:
    RESET   = "\033[0m"    # Cancel all formatting — always add this at the end
    BOLD    = "\033[1m"    # Make text heavier / brighter
    DIM     = "\033[2m"    # Make text lighter / faded
    RED     = "\033[91m"   # Bright red   — used for errors
    GREEN   = "\033[92m"   # Bright green — used for success messages
    YELLOW  = "\033[93m"   # Bright yellow — used for warnings and menu numbers
    BLUE    = "\033[94m"   # Bright blue  — used for headers and borders
    MAGENTA = "\033[95m"   # Bright magenta — used for batch upload labels
    CYAN    = "\033[96m"   # Bright cyan  — used for prompts and info messages
    WHITE   = "\033[97m"   # Bright white


# ─── Convenience printer helpers ──────────────────────────────────────────────

def _p(color: str, icon: str, msg: str) -> None:
    """
    Internal helper that all the printer functions below call.
    Wraps the message in the given colour, prepends an icon, then resets.

    Example output:   ✓  File uploaded successfully
    """
    # f-string combines colour code + icon + space + message + reset code
    print(f"{color}{icon}  {msg}{C.RESET}")

# Four clean one-liners that the rest of the code uses for printing.
# Each maps to a semantic meaning so the code reads like English.

def ok(msg: str)  : _p(C.GREEN,  "  ✓", msg)   # success   → green checkmark
def err(msg: str) : _p(C.RED,    "  ✗", msg)   # error     → red cross
def info(msg: str): _p(C.CYAN,   "  ℹ", msg)   # info      → cyan info symbol
def warn(msg: str): _p(C.YELLOW, "  ⚠", msg)   # warning   → yellow warning


def banner(title: str) -> None:
    """
    Print a decorated section header.

    Output looks like:
      ══════════════════════════════════════════════════════
         Upload File
      ══════════════════════════════════════════════════════
    """
    w      = 58                                    # total width of the border line
    border = f"{C.BOLD}{C.BLUE}{'═' * w}{C.RESET}" # '═' repeated w times
    print(f"\n{border}")
    print(f"{C.BOLD}{C.WHITE}   {title}{C.RESET}")  # title indented 3 spaces
    print(border)


# ══════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION
#  These are the settings you'll most likely want to change.
# ══════════════════════════════════════════════════════════════════════════════

# The IP address of the machine running server.py.
# "127.0.0.1" means "this same computer" (loopback / localhost).
# Change this to the server's actual LAN or public IP to connect remotely.
DEFAULT_IP   = "127.0.0.1"

# TCP port where the server is listening.  Must match HOST_PORT in server.py.
DEFAULT_PORT = 1234

# Character encoding for all text sent over the network.
# UTF-8 handles every language, emoji, and special character safely.
ENCODER = "utf-8"

# How many bytes we read/write in one go.
# 131072 bytes = 128 KB.  Bigger chunks = faster transfers because the program
# spends less time in Python and more time in the fast kernel networking code.
# This MUST match the server's BYTESIZE or the transfer protocol breaks.
BYTESIZE = 131072       # 128 KB

# Where downloaded files are saved on the client machine.
DOWNLOAD_DIR = "client_downloads"

# Auto-reconnect settings:
# MAX_RETRIES — how many times to attempt reconnecting before giving up.
MAX_RETRIES  = 4

# RETRY_BASE — the base for exponential back-off.
# Attempt 1: wait 1.5^1 = 1.5 s
# Attempt 2: wait 1.5^2 = 2.25 s
# Attempt 3: wait 1.5^3 = 3.375 s
# This prevents hammering a server that is still starting up.
RETRY_BASE   = 1.5

# Create the downloads directory if it doesn't exist yet.
# exist_ok=True means: don't crash if the folder is already there.
os.makedirs(DOWNLOAD_DIR, exist_ok=True)


# ══════════════════════════════════════════════════════════════════════════════
#  LENGTH-PREFIXED PROTOCOL
#
#  The Problem with raw recv():
#    TCP is a *stream* protocol, not a *message* protocol.
#    That means when we call recv(1024), we might get:
#      - Less than 1024 bytes (the rest arrives in the next recv call)
#      - Exactly 1024 bytes
#      - Parts of two messages merged together
#
#    This is called "TCP framing" and it is the #1 cause of bugs in beginner
#    socket programs.  The original code used recv(BYTESIZE) everywhere and
#    suffered from exactly this problem — messages sometimes arrived garbled.
#
#  The Solution — Length Prefix:
#    Before every message we send exactly 4 bytes that say how long the
#    message is (as a big-endian integer).  The receiver reads those 4 bytes
#    first, learns the exact length, then reads precisely that many bytes.
#    No guessing, no merging, no splitting.
#
#    Visual:
#      [ 0x00 0x00 0x00 0x05 ]  [ H e l l o ]
#        ↑ 4-byte length=5       ↑ 5-byte payload
#
#  This protocol is IDENTICAL on both client and server — they must match.
# ══════════════════════════════════════════════════════════════════════════════

def _recv_n(sock: socket.socket, n: int) -> bytes:
    """
    Read *exactly* n bytes from the socket, blocking until done or error.

    Why can't we just call sock.recv(n)?
      Because recv(n) says "give me UP TO n bytes" — it can return fewer
      if the kernel hasn't assembled all of them yet.  This function loops
      until it has collected exactly the right amount.

    Parameters:
      sock — the connected TCP socket to read from
      n    — the exact number of bytes we want

    Returns:
      A bytes object of exactly length n.

    Raises:
      ConnectionError if the socket closes before we got all n bytes.
    """
    # bytearray is a mutable (changeable) sequence of bytes.
    # We use it instead of bytes because we need to append chunks to it.
    buf = bytearray()

    # Keep reading until we have accumulated n bytes total.
    while len(buf) < n:
        # How many bytes are still missing?
        remaining = n - len(buf)

        # Request at most BYTESIZE bytes per recv() call.
        # min() ensures we don't ask for more than what's still missing
        # even if remaining is tiny (e.g. 3 bytes).
        to_read = min(remaining, BYTESIZE)

        # sock.recv(to_read) asks the OS for up to to_read bytes.
        # This call BLOCKS — the program pauses here until data arrives.
        chunk = sock.recv(to_read)

        # If recv() returns an empty bytes object b"" it means the server
        # has closed the connection.  We can't recover, so raise an error.
        if not chunk:
            raise ConnectionError("Server closed the connection")

        # Append the received bytes to our growing buffer.
        buf += chunk

    # Convert the mutable bytearray to an immutable bytes object before
    # returning.  This is the standard Python convention.
    return bytes(buf)


def recv_msg(sock: socket.socket) -> str:
    """
    Receive exactly one complete message from the server.

    Steps:
      1. Read 4 bytes → convert them to an integer → that is the message length.
      2. Read exactly that many bytes → decode UTF-8 → return as a Python string.
    """
    # int.from_bytes(data, "big") converts 4 raw bytes into a Python integer.
    # "big" means big-endian byte order (most significant byte first).
    # Example: bytes [0x00, 0x00, 0x00, 0x05] → integer 5
    length = int.from_bytes(_recv_n(sock, 4), "big")

    # Now read exactly 'length' bytes and decode them from UTF-8 to a string.
    return _recv_n(sock, length).decode(ENCODER)


def send_msg(sock: socket.socket, text: str) -> None:
    """
    Send exactly one complete message to the server.

    Steps:
      1. Encode the string to UTF-8 bytes.
      2. Prepend its length as 4 big-endian bytes.
      3. Call sendall() to guarantee every byte reaches the server.

    Why sendall() and not send()?
      send() might only send part of the data if the kernel's send buffer is
      temporarily full.  sendall() loops internally until everything is sent
      or an exception is raised — much safer.
    """
    # Encode string → bytes using UTF-8
    data = text.encode(ENCODER)

    # len(data).to_bytes(4, "big") converts the integer length to 4 bytes.
    # Example: 5 → b'\x00\x00\x00\x05'
    # The '+' concatenates the 4-byte prefix with the actual message bytes.
    # sendall() sends the whole thing in one kernel call.
    sock.sendall(len(data).to_bytes(4, "big") + data)


# ─── Utility functions ────────────────────────────────────────────────────────

def sha256_file(path: str) -> str:
    """
    Compute the SHA-256 hash of a local file, reading in BYTESIZE chunks.

    SHA-256 is a cryptographic hash function.  It turns any file into a
    64-character hex string (called a "digest" or "fingerprint").
    Even a single flipped bit changes the hash completely.

    We use it to verify file integrity:
      • Hash the file BEFORE sending → call this H1.
      • The receiver hashes the received data → call that H2.
      • If H1 == H2, the file arrived perfectly intact.
      • If H1 != H2, something got corrupted and we delete the bad file.

    Why read in chunks instead of reading the whole file at once?
      If the file is 4 GB and we do f.read() we load 4 GB into RAM.
      Reading 128 KB at a time means we only ever use 128 KB of RAM,
      regardless of file size.
    """
    # Create a new SHA-256 hash object.  Think of it as an empty blender
    # that we'll feed data into piece by piece.
    h = hashlib.sha256()

    # Open the file in binary mode ('rb') — we want raw bytes, not text.
    with open(path, "rb") as f:
        # iter(callable, sentinel) calls callable repeatedly until it returns sentinel.
        # lambda: f.read(BYTESIZE)  → reads 128 KB from file each call
        # b""                       → stops when the file is exhausted (EOF)
        for chunk in iter(lambda: f.read(BYTESIZE), b""):
            # Feed each chunk into the hash object.
            # The hash is computed incrementally — order matters.
            h.update(chunk)

    # hexdigest() returns the final hash as a 64-character hex string.
    # Example: "a3f5b2c1d9e7..."
    return h.hexdigest()


def fmt_size(n: int | float) -> str:
    """
    Convert a byte count to a human-readable size string.

    Examples:
      500       → "500.0 B"
      2_097_152 → "2.0 MB"
      1_073_741_824 → "1.0 GB"

    Algorithm: keep dividing by 1024 until n < 1024, then use that unit.
    """
    # Try each unit in order from smallest to largest.
    for unit in ("B", "KB", "MB", "GB"):
        # If n fits in this unit (less than 1024), format and return.
        if n < 1024:
            return f"{n:.1f} {unit}"
        # Otherwise divide by 1024 to go to the next unit up.
        n /= 1024
    # If we've gone through all four units, what's left is terabytes.
    return f"{n:.1f} TB"


def progress_bar(done: int, total: int, speed: float, width: int = 36) -> str:
    """
    Build a visual progress bar as a string — no external libraries needed.

    Parameters:
      done  — bytes transferred so far
      total — total bytes to transfer
      speed — current speed in MB/s
      width — number of characters for the filled/empty part of the bar

    Example output:
      [██████████░░░░░░░░░░░░░░░░░░░░░░░░░░]  27.8%    30.0 MB/100.0 MB   45.23 MB/s

    How the bar is drawn:
      pct  = 0.278  (27.8% done)
      fill = int(36 * 0.278) = 10    → 10 filled blocks '█'
      rest = 36 - 10 = 26            → 26 empty blocks  '░'
    """
    # Calculate completion fraction (0.0 to 1.0).
    # If total is 0 (empty file), treat it as complete (1.0) to avoid ZeroDivisionError.
    pct  = done / total if total else 1.0

    # How many '█' characters to draw for the filled portion.
    fill = int(width * pct)

    # Build the bar string: filled part + empty part.
    bar  = f"{'█' * fill}{'░' * (width - fill)}"

    # Assemble the full display line with colours.
    return (
        f"{C.CYAN}[{bar}]{C.RESET} "           # coloured bar
        f"{C.BOLD}{pct*100:5.1f}%{C.RESET}  "  # percentage, e.g. " 27.8%"
        f"{fmt_size(done):>10}/{fmt_size(total):<10}  "  # "30.0 MB/100.0 MB"
        f"{C.YELLOW}{speed:6.2f} MB/s{C.RESET}" # speed, e.g. "45.23 MB/s"
    )


def print_progress(done: int, total: int, t0: float) -> None:
    """
    Print the progress bar, overwriting the SAME terminal line each call.

    "\r" (carriage return) moves the cursor back to the start of the current
    line WITHOUT moving to the next line.  Combined with end="" (no newline)
    and flush=True (force output immediately), this makes the bar animate
    in place instead of printing hundreds of new lines.
    """
    # Calculate how many seconds have passed since the transfer started.
    # max(..., 1e-9) prevents dividing by zero if t0 was literally just set.
    elapsed = max(time.perf_counter() - t0, 1e-9)

    # Speed in MB/s: bytes transferred ÷ seconds ÷ bytes_per_MB
    speed   = done / elapsed / 1_048_576  # 1_048_576 = 1024 * 1024

    # \r moves cursor to start of line; end="" prevents a newline; flush forces output
    print(f"\r  {progress_bar(done, total, speed)}", end="", flush=True)


def unique_local_path(filename: str) -> str:
    """
    Return a safe local path for a downloaded file.

    If client_downloads/photo.jpg already exists, we return
    client_downloads/photo_20250510_143022.jpg instead, so the old file
    is never silently overwritten.

    os.path.basename() strips any directory path from the filename,
    so a server can't trick us into writing outside DOWNLOAD_DIR by
    sending something like "../../evil.sh".
    """
    # Combine the download directory with the safe filename.
    dest = os.path.join(DOWNLOAD_DIR, os.path.basename(filename))

    # If no file exists at that path, we're free to use it.
    if not os.path.exists(dest):
        return dest

    # File already exists — append a timestamp to avoid collision.
    # os.path.splitext("photo.jpg") → ("photo", ".jpg")
    name, ext = os.path.splitext(os.path.basename(filename))

    # Format: YYYYMMDD_HHMMSS  e.g. "20250510_143022"
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Result: "client_downloads/photo_20250510_143022.jpg"
    return os.path.join(DOWNLOAD_DIR, f"{name}_{ts}{ext}")


# ══════════════════════════════════════════════════════════════════════════════
#  CONNECTION  — with auto-reconnect and exponential back-off
# ══════════════════════════════════════════════════════════════════════════════

# These two globals hold the current server address.
# They start as the defaults and get updated in main() when the user types in
# a custom IP/port.  Using module-level globals here keeps them accessible to
# the reconnect logic without passing them through every function.
_server_ip   = DEFAULT_IP
_server_port = DEFAULT_PORT


def _make_socket() -> socket.socket:
    """
    Create a TCP socket pre-tuned for low latency and high throughput.

    ── TCP_NODELAY (Nagle's algorithm disabled) ──────────────────────────────
    By default, TCP uses Nagle's algorithm: it collects small outgoing packets
    and batches them into one bigger packet.  This saves bandwidth but adds
    delay — up to ~200ms on a slow network.

    For our command/response protocol (we send "PING" and wait for "PONG"),
    this delay is very noticeable.  TCP_NODELAY tells the kernel:
      "Send every packet immediately, even if it's tiny."

    Result: commands feel instant instead of sluggish.

    ── SO_SNDBUF / SO_RCVBUF ─────────────────────────────────────────────────
    These are the kernel's internal send and receive buffers.  Think of them
    as waiting rooms for outgoing and incoming data.

    The defaults (typically 64–256 KB) can cause the sender to pause and wait
    for the receiver to drain the buffer during a big file transfer.

    Bumping both to 1 MB (1 << 20 = 1,048,576 bytes) gives the kernel more
    space to queue data, allowing transfers to proceed without pause.

    ── SO_KEEPALIVE ──────────────────────────────────────────────────────────
    If a machine goes offline without sending a TCP FIN packet (power cut,
    network drop, Wi-Fi dropout), the other side of a TCP connection will
    just sit there forever waiting for data that never comes.

    SO_KEEPALIVE tells the OS to periodically send a tiny "are you still there?"
    probe.  If the probe gets no reply after a few tries, the OS marks the
    socket as dead and raises an error so our code can react.
    """
    # AF_INET  = IPv4 addresses
    # SOCK_STREAM = TCP (reliable, ordered, byte-stream)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # IPPROTO_TCP says the option applies to the TCP layer specifically.
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY,  1)        # disable Nagle

    # SOL_SOCKET says the option applies to the socket layer generally.
    s.setsockopt(socket.SOL_SOCKET,  socket.SO_SNDBUF,    1 << 20)  # 1 MB send buffer
    s.setsockopt(socket.SOL_SOCKET,  socket.SO_RCVBUF,    1 << 20)  # 1 MB recv buffer
    s.setsockopt(socket.SOL_SOCKET,  socket.SO_KEEPALIVE, 1)        # detect dead peers

    return s  # return the configured socket, not yet connected


def connect(ip: str, port: int, silent: bool = False) -> socket.socket | None:
    """
    Attempt to connect to the server up to MAX_RETRIES times.

    If each attempt fails, we wait progressively longer before the next try
    (exponential back-off) so we don't spam a server that's still starting up.

    Parameters:
      ip     — server IP address string, e.g. "192.168.1.5"
      port   — server port, e.g. 1234
      silent — if True, suppress the "Connecting…" info message

    Returns:
      A connected, configured socket.socket on success, or None on failure.
    """
    # range(1, MAX_RETRIES + 1) gives us [1, 2, 3, 4] — attempt numbers.
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if not silent:
                info(f"Connecting to {ip}:{port}  (attempt {attempt}/{MAX_RETRIES})…")

            # Create a fresh, tuned socket for each attempt.
            # (Can't reuse a socket that failed to connect.)
            s = _make_socket()

            # settimeout(8) means: if connect() doesn't complete within 8 seconds,
            # raise a socket.timeout exception instead of waiting forever.
            s.settimeout(8)

            # Actually initiate the TCP three-way handshake with the server.
            # This call blocks until connected or until the timeout fires.
            s.connect((ip, port))

            # Switch the socket back to blocking mode with no timeout.
            # After connecting we want recv() to block indefinitely (the server
            # might take a moment to respond, and that's fine).
            s.settimeout(None)

            # The server sends a WELCOME message immediately after accepting.
            # recv_msg reads and decodes it.
            welcome = recv_msg(s)

            # The welcome message is pipe-separated:
            # "WELCOME|Synapse v2|192.168.1.5:1234|3"
            #   parts[0] = "WELCOME"
            #   parts[1] = app name + version
            #   parts[2] = server address
            #   parts[3] = number of clients currently online
            parts = welcome.split("|")
            if parts[0] == "WELCOME":
                ok(f"Connected to  {parts[1]}  at  {parts[2]}")
                ok(f"Clients online: {parts[3]}")

            # Return the live socket so the caller can use it.
            return s

        except ConnectionRefusedError:
            # Server is not running yet, or the port is wrong.
            warn(f"Attempt {attempt} failed — server refused connection")
        except socket.timeout:
            # Timed out — server unreachable within 8 seconds.
            warn(f"Attempt {attempt} timed out after 8 s")
        except Exception as e:
            # Any other error (wrong IP format, network down, etc.)
            warn(f"Attempt {attempt} error: {e}")

        # Don't sleep after the very last attempt — just exit the loop.
        if attempt < MAX_RETRIES:
            # Calculate the wait time using exponential back-off:
            # attempt=1 → 1.5^1 = 1.5 s
            # attempt=2 → 1.5^2 = 2.25 s
            # attempt=3 → 1.5^3 = 3.375 s
            delay = RETRY_BASE ** attempt
            info(f"Retrying in {delay:.1f}s…")
            time.sleep(delay)   # pause before the next attempt

    # All attempts exhausted — signal failure by returning None.
    err("Could not connect after all attempts.")
    return None


def reconnect(ip: str, port: int) -> socket.socket | None:
    """
    Called when a command loses its connection mid-session.
    Simply wraps connect() with a warning message.
    """
    warn("Connection lost — attempting reconnect…")
    return connect(ip, port, silent=False)


# ══════════════════════════════════════════════════════════════════════════════
#  COMMAND IMPLEMENTATIONS
#  Each function handles one menu option end-to-end:
#    1. Tell the server which command we want (send_msg).
#    2. Exchange the necessary data according to the protocol.
#    3. Show the user the result.
# ══════════════════════════════════════════════════════════════════════════════

# ─── UPLOAD ONE FILE ──────────────────────────────────────────────────────────

def _upload_one(sock: socket.socket, path: str, label: str = "") -> bool:
    """
    Upload a single file to the server.

    This is a shared helper used by both cmd_send_file() (single upload)
    and cmd_send_multi() (batch upload).  Splitting the logic out avoids
    copy-pasting the same 40 lines.

    Protocol after the caller has already sent "SEND_FILE" or "SEND_MULTI"
    and received "READY":
      Client → filename   (just the base name, no directory)
      Client → filesize   (as a string of digits, e.g. "104857600")
      Client → sha256     (64-char hex string)
      Client → [raw bytes × filesize]  ← no framing, just raw data
      Server → "OK|saved_name|speed"   on success
      Server → "FAIL|reason"           on failure

    Parameters:
      sock  — the connected socket
      path  — full local path to the file
      label — optional display label for progress (used in batch to show [2/5])

    Returns:
      True if the server confirmed success, False otherwise.
    """
    # os.path.basename("C:/photos/cat.jpg") → "cat.jpg"
    filename = os.path.basename(path)

    # os.path.getsize() returns the file's byte count without opening it.
    filesize = os.path.getsize(path)

    # Hash the file before sending — we'll compare this with the server's hash.
    info(f"Hashing  {label or filename}  ({fmt_size(filesize)})…")
    fhash = sha256_file(path)
    ok(f"SHA-256: {fhash[:20]}…")   # only show first 20 chars to keep it tidy

    # Send the three pieces of metadata the server needs to prepare for the file.
    send_msg(sock, filename)        # e.g. "cat.jpg"
    send_msg(sock, str(filesize))   # e.g. "104857600"
    send_msg(sock, fhash)           # 64-char SHA-256 hex

    info(f"Uploading  {label or filename}…")

    sent  = 0             # bytes sent so far
    t0    = time.perf_counter()  # start the timer for speed calculation
    last  = t0            # when was the bar last updated?

    # Open the file in binary read mode.
    with open(path, "rb") as f:
        # iter(lambda: f.read(BYTESIZE), b"") reads 128 KB at a time
        # and stops when f.read() returns b"" (end of file).
        for chunk in iter(lambda: f.read(BYTESIZE), b""):
            # sendall() ensures every byte of this chunk is transmitted.
            sock.sendall(chunk)

            # Track how many bytes we've sent.
            sent += len(chunk)

            # Throttle progress bar updates to ~6 per second (every 150 ms).
            # Without this throttle, print_progress would be called thousands
            # of times per second for fast transfers, wasting CPU.
            now = time.perf_counter()
            if now - last >= 0.15:
                print_progress(sent, filesize, t0)
                last = now   # reset the "last update" timer

    # Print the bar one final time showing 100% completion.
    print_progress(filesize, filesize, t0)
    print()   # move to the next line after the progress bar

    # Wait for the server's verdict.
    result = recv_msg(sock)

    # Parse pipe-separated response: "OK|saved_name|speed" or "FAIL|reason"
    parts = result.split("|")
    if parts[0] == "OK":
        # parts[1] = name the server saved the file as (may differ if duplicate)
        # parts[2] = measured upload speed in MB/s (as a string)
        speed = float(parts[2]) if len(parts) > 2 else 0.0
        ok(f"Saved as '{parts[1]}'  @ {speed:.2f} MB/s")
        return True   # success
    else:
        err(f"Upload failed: {'|'.join(parts[1:])}")
        return False  # failure


def cmd_send_file(sock: socket.socket) -> None:
    """
    Menu option 1 — Upload a single file.

    Asks the user for a path, then calls _upload_one() to do the actual work.
    """
    banner("Upload File")

    # input() displays the prompt and waits for the user to type + press Enter.
    # .strip() removes leading/trailing spaces.
    # .strip('"') removes quotation marks Windows sometimes adds when you
    # drag-and-drop a file into the terminal.
    path = input(f"{C.CYAN}  File path: {C.RESET}").strip().strip('"')

    # If the user just pressed Enter without typing, cancel gracefully.
    if not path:
        warn("Cancelled.")
        return

    # Check the file exists before wasting time connecting.
    if not os.path.isfile(path):
        err(f"Not found: {path}")
        return

    # Tell the server we want to upload a file.
    send_msg(sock, "SEND_FILE")

    # Wait for the server to say it's ready.
    if recv_msg(sock) != "READY":
        err("Server not ready for upload.")
        return

    # Hand off to the shared upload logic.
    _upload_one(sock, path)


# ─── BATCH UPLOAD ─────────────────────────────────────────────────────────────

def cmd_send_multi(sock: socket.socket) -> None:
    """
    Menu option 2 — Upload multiple files or an entire folder in one go.

    The user can enter:
      • A single file path
      • Multiple file paths separated by commas
      • A folder path (all files inside are queued)
      • Any mix of the above

    Protocol extension (SEND_MULTI):
      Client → "SEND_MULTI"
      Server → "READY"
      Client → count   (number of files as a string)
      For each file:
        [same metadata + raw bytes as _upload_one]
        Server → "OK|…" or "FAIL|…"
      Server → "MULTI_DONE|n_ok|n_fail"
    """
    banner("Batch Upload")
    raw = input(f"{C.CYAN}  File paths (comma-separated) or folder: {C.RESET}").strip().strip('"')

    if not raw:
        warn("Cancelled.")
        return

    # Collect all file paths we'll upload.
    paths: list[str] = []

    # Split the user's input by comma to handle multiple entries.
    for entry in raw.split(","):
        # Clean up each entry individually.
        entry = entry.strip().strip('"')

        if os.path.isfile(entry):
            # It's a single file — add it directly.
            paths.append(entry)

        elif os.path.isdir(entry):
            # It's a folder — add every regular file inside it.
            # sorted() gives consistent ordering (alphabetical).
            for f in sorted(os.listdir(entry)):
                full = os.path.join(entry, f)         # build full path
                if os.path.isfile(full):               # skip subdirectories
                    paths.append(full)

        else:
            # Neither a file nor a folder — warn the user and skip.
            warn(f"Skipping (not found): {entry}")

    # If we ended up with nothing to upload, stop here.
    if not paths:
        err("No valid files found.")
        return

    info(f"{len(paths)} file(s) queued for upload.")

    # Initiate the SEND_MULTI protocol.
    send_msg(sock, "SEND_MULTI")
    if recv_msg(sock) != "READY":
        err("Server not ready for batch upload.")
        return

    # Tell the server exactly how many files are coming so it knows when to stop.
    send_msg(sock, str(len(paths)))

    # Track success/failure counts.
    n_ok = n_fail = 0

    # Upload each file in turn.
    # enumerate(paths, 1) gives (1, path1), (2, path2), etc.
    for i, path in enumerate(paths, 1):
        # Visual separator for each file in the batch.
        print(f"\n{C.BOLD}{C.MAGENTA}  [{i}/{len(paths)}] {os.path.basename(path)}{C.RESET}")

        # _upload_one() handles the metadata + data + server response for one file.
        success = _upload_one(sock, path, label=f"[{i}/{len(paths)}]")
        if success:
            n_ok   += 1
        else:
            n_fail += 1

    # After all files, the server sends a summary — receive it (we don't display
    # it directly because we're already tracking n_ok/n_fail ourselves).
    recv_msg(sock)   # "MULTI_DONE|n_ok|n_fail" — acknowledged but our count is authoritative

    print()
    ok(f"Batch complete: {n_ok} succeeded, {n_fail} failed")


# ─── LIST SERVER FILES ────────────────────────────────────────────────────────

def cmd_list_server_files(sock: socket.socket) -> None:
    """
    Menu option 3 — Show what files are stored on the server (with sizes).
    This does NOT download anything.

    Protocol:
      Client → "LIST_FILES"
      Server → "NOFILES"
      Server → "FILELIST|name1:size1|name2:size2|…"
    """
    banner("Files on Server")

    # Ask the server for its file list.
    send_msg(sock, "LIST_FILES")
    resp = recv_msg(sock)

    if resp == "NOFILES":
        warn("Server has no files stored.")
        return

    # Parse the pipe-separated response.
    parts = resp.split("|")
    if parts[0] != "FILELIST":
        err(f"Unexpected response: {resp}")
        return

    # parts[1:] = ["file1.jpg:102400", "doc.pdf:5120000", …]
    entries = parts[1:]

    # Print a formatted table header.
    print(f"\n  {'#':>3}   {'Filename':<40}  {'Size':>10}")
    print(f"  {'─'*3}   {'─'*40}  {'─'*10}")

    for i, entry in enumerate(entries, 1):
        # Each entry is "filename:bytecount" — split on the LAST colon
        # in case the filename itself contains colons.
        name, size_str = entry.rsplit(":", 1)
        size_human     = fmt_size(int(size_str))

        # :<40 left-aligns name in a 40-char column
        # :>10 right-aligns size in a 10-char column
        print(f"  {C.YELLOW}{i:>3}{C.RESET}   {name:<40}  {C.DIM}{size_human:>10}{C.RESET}")
    print()


# ─── DOWNLOAD A FILE ──────────────────────────────────────────────────────────

def cmd_get_file(sock: socket.socket) -> None:
    """
    Menu option 4 — Download a file from the server.

    Protocol:
      Client → "GET_FILE"
      Server → "NOFILES"                        (nothing to download)
      Server → "FILES|file1|file2|…"            (available file list)
      Client → choice (1-based number) | "CANCEL"
      Server → "ERROR|reason"                   (bad choice)
      Server → "META|name|size|sha256"          (file info)
      Client → "ACK" | "NACK"                   (proceed or cancel)
      Server → [raw bytes × size]               (file data after ACK)
    """
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

    # parts[1:] is a list of filenames available on the server.
    files = parts[1:]

    # Display the file list as a numbered menu.
    print(f"\n  {'#':>3}   Filename")
    print(f"  {'─'*3}   {'─'*40}")
    for i, f in enumerate(files, 1):
        print(f"  {C.YELLOW}{i:>3}{C.RESET}   {f}")

    # Ask the user which file they want.
    choice = input(f"\n{C.CYAN}  File number (Enter to cancel): {C.RESET}").strip()

    if not choice:
        # User pressed Enter without a number — cancel politely.
        send_msg(sock, "CANCEL")
        warn("Cancelled.")
        return

    # Send the user's number to the server (e.g. "3").
    send_msg(sock, choice)

    # The server sends back the file's metadata (or an error).
    meta = recv_msg(sock)

    if meta.startswith("ERROR"):
        err(meta.split("|", 1)[1])   # show just the reason text
        return

    # "META|filename|bytecount|sha256hash"
    mparts   = meta.split("|")
    fname    = mparts[1]       # file name the server will send
    filesize = int(mparts[2])  # exact byte count we expect to receive
    expected = mparts[3]       # SHA-256 digest to verify against

    # Show the user what they're about to download.
    info(f"File     : {fname}")
    info(f"Size     : {fmt_size(filesize)}")
    info(f"SHA-256  : {expected[:20]}…")

    # Prepare a safe local path (adds timestamp if filename already exists).
    dest = unique_local_path(fname)

    # Tell the server we're ready — it will start sending bytes immediately.
    send_msg(sock, "ACK")

    info("Downloading…")

    # Track download progress.
    received = 0
    hasher   = hashlib.sha256()   # build the hash as we receive data
    t0       = time.perf_counter()
    last     = t0

    try:
        with open(dest, "wb") as f:
            # Keep reading until we have received every byte.
            while received < filesize:
                # Ask for at most BYTESIZE bytes, but don't overshoot filesize.
                # This is CRITICAL — without min(), recv() might grab bytes
                # from the NEXT protocol message (the server's next send_msg).
                chunk = sock.recv(min(BYTESIZE, filesize - received))

                if not chunk:
                    # Server closed the connection before we got everything.
                    raise ConnectionError("Server closed connection mid-download")

                # Write each chunk to disk immediately (streaming).
                f.write(chunk)

                # Update the hash with this chunk.
                hasher.update(chunk)

                # Track total received.
                received += len(chunk)

                # Throttle progress bar updates to ~6 per second.
                now = time.perf_counter()
                if now - last >= 0.15:
                    print_progress(received, filesize, t0)
                    last = now

        # Final bar update showing 100%.
        print_progress(filesize, filesize, t0)
        print()

    except Exception as e:
        print()   # break out of the progress bar line
        if os.path.exists(dest):
            os.remove(dest)   # delete the partial/corrupt file
        err(f"Download failed: {e}")
        return

    # Verify integrity: did we get exactly the right bytes?
    if hasher.hexdigest() == expected and received == filesize:
        elapsed = max(time.perf_counter() - t0, 1e-9)
        speed   = received / elapsed / 1_048_576
        ok(f"Downloaded '{os.path.basename(dest)}'  @ {speed:.2f} MB/s")
        ok(f"Saved to:   {os.path.abspath(dest)}")
    else:
        # Something got corrupted — delete the bad file and warn the user.
        os.remove(dest)
        err("Hash mismatch — corrupted file discarded. Try again.")


# ─── GROUP CHAT ───────────────────────────────────────────────────────────────

def cmd_chat(sock: socket.socket) -> None:
    """
    Menu option 5 — Join the group chat.

    The server broadcasts each message to ALL other connected clients,
    so this works as a multi-user chat room.

    The tricky part: we need to SEND and RECEIVE at the same time.
    If we do them one after the other, the program freezes:
      • Waiting for user input → can't receive server messages
      • Waiting for server messages → can't accept user input

    Solution: run a background thread that does nothing but listen for
    incoming messages and print them.  The main thread handles user input.
    The threading.Event 'stop_flag' is the shared signal that tells the
    listener thread to stop when the chat session ends.

    Protocol:
      Client → "CHAT"
      Server → "CHAT_START"
      loop:
        Client → message text  |  "CHAT_QUIT"
        Server → "[HH:MM:SS] name: message"  (broadcast to all)
      Server → "CHAT_END"
    """
    banner("Group Chat")

    # Tell the server we want to enter chat mode.
    send_msg(sock, "CHAT")

    # Wait for the server to confirm it's in chat mode too.
    resp = recv_msg(sock)
    if resp != "CHAT_START":
        err(f"Unexpected: {resp}")
        return

    info("Chat started.  Type messages and press Enter.  Type 'quit' to leave.\n")

    # threading.Event is like a shared on/off switch between threads.
    # stop_flag.is_set() → True means "stop"
    # stop_flag.set()    → flip it to "stop"
    # stop_flag.clear()  → flip it back to "go"
    stop_flag = threading.Event()

    def _listener():
        """
        Background thread function — runs concurrently with the main input loop.

        While the main thread blocks on input(), this thread blocks on recv_msg().
        Because they're in different threads, both can wait at the same time
        without blocking each other.
        """
        # Keep looping until the stop signal is set OR an error occurs.
        while not stop_flag.is_set():
            try:
                msg = recv_msg(sock)
            except Exception:
                # Socket error (disconnected, server crashed, etc.) — stop quietly.
                stop_flag.set()
                break

            if msg == "CHAT_END":
                # Server ended the chat session — signal the main thread too.
                stop_flag.set()
                break

            # Print the incoming message.
            # \r at the start clears any partially typed input line.
            # After printing, we reprint the "You: " prompt so it looks clean.
            print(f"\r{C.GREEN}  {msg}{C.RESET}\n{C.CYAN}  You: {C.RESET}", end="", flush=True)

    # Start the listener in a daemon thread.
    # daemon=True means: if the main program exits, this thread is killed automatically
    # instead of keeping the program alive.
    listener_thread = threading.Thread(target=_listener, daemon=True)
    listener_thread.start()

    # Main thread: read user input in a loop.
    while not stop_flag.is_set():
        try:
            msg = input(f"{C.CYAN}  You: {C.RESET}").strip()
        except (EOFError, KeyboardInterrupt):
            # User pressed Ctrl+C or Ctrl+D — treat it as a quit request.
            msg = "quit"

        # Check for quit commands (case-insensitive).
        if msg.lower() in ("quit", "exit", "q", ""):
            send_msg(sock, "CHAT_QUIT")   # tell the server we're leaving
            stop_flag.set()               # signal the listener thread to stop
            break

        # Send the message to the server (which will broadcast it).
        send_msg(sock, msg)

    # Wait for the listener thread to finish (up to 2 seconds).
    # Without join(), the thread might still be running when we return.
    listener_thread.join(timeout=2)

    ok("Left the chat.")


# ─── PING ─────────────────────────────────────────────────────────────────────

def cmd_ping(sock: socket.socket) -> None:
    """
    Menu option 6 — Measure round-trip time (RTT) to the server.

    RTT (Round-Trip Time) is the time between sending a packet and receiving
    the reply.  A low RTT means the network is fast and responsive.
    A high RTT means there is latency (lag).

    We send 5 PINGs, measure each one, and report min/max/avg.

    Protocol:
      Client → "PING"
      Server → "PONG"
    """
    banner("Ping")
    samples = []   # list of RTT measurements in milliseconds

    for i in range(5):
        # Record the time just before sending.
        t0 = time.perf_counter()

        send_msg(sock, "PING")
        resp = recv_msg(sock)

        # Calculate how many milliseconds the round-trip took.
        # * 1000 converts seconds to milliseconds.
        rtt = (time.perf_counter() - t0) * 1000

        if resp == "PONG":
            samples.append(rtt)
            status = f"{C.GREEN}pong{C.RESET}"
        else:
            status = f"{C.RED}??{C.RESET}"   # unexpected response

        print(f"  [{i+1}] {status}   RTT = {rtt:.2f} ms")

        # Small pause between pings so we don't flood the server.
        time.sleep(0.1)

    if samples:
        avg = sum(samples) / len(samples)   # arithmetic mean
        mn  = min(samples)
        mx  = max(samples)
        print(f"\n  {C.BOLD}avg {avg:.2f} ms   min {mn:.2f} ms   max {mx:.2f} ms{C.RESET}")


# ─── SERVER INFO ──────────────────────────────────────────────────────────────

def cmd_server_info(sock: socket.socket) -> None:
    """
    Menu option 7 — Ask the server for its health status.

    Protocol:
      Client → "SERVER_INFO"
      Server → "INFO|uptime|client_count|file_count|disk_free_MB"
    """
    banner("Server Info")
    send_msg(sock, "SERVER_INFO")
    resp = recv_msg(sock)

    parts = resp.split("|")
    if parts[0] != "INFO":
        err(f"Unexpected: {resp}")
        return

    # Unpack the five parts of the INFO response.
    uptime, clients, files, disk_mb = parts[1], parts[2], parts[3], parts[4]

    print(f"\n  Uptime       :  {C.CYAN}{uptime}{C.RESET}")
    print(f"  Clients      :  {C.CYAN}{clients}{C.RESET}")
    print(f"  Stored files :  {C.CYAN}{files}{C.RESET}")
    # float() because disk_mb is a string like "45231.0"
    print(f"  Disk free    :  {C.CYAN}{float(disk_mb):.0f} MB{C.RESET}\n")


# ─── SET DISPLAY NAME ─────────────────────────────────────────────────────────

def cmd_set_name(sock: socket.socket) -> None:
    """
    Menu option 8 — Set your display name for the group chat.

    Protocol:
      Client → "SET_NAME|yourname"
      Server → "NAME_OK|yourname"  or error
    """
    name = input(f"{C.CYAN}  Your chat name (max 32 chars): {C.RESET}").strip()
    if not name:
        warn("Cancelled.")
        return

    # [:32] — Python slice: take at most 32 characters.
    # Even if the user types 100 chars, we only send the first 32.
    send_msg(sock, f"SET_NAME|{name[:32]}")

    resp  = recv_msg(sock)
    parts = resp.split("|")
    if parts[0] == "NAME_OK":
        ok(f"Name set to '{parts[1]}'")
    else:
        err(f"Unexpected: {resp}")


# ─── LIST LOCAL DOWNLOADS ─────────────────────────────────────────────────────

def cmd_list_local() -> None:
    """
    Menu option 9 — Show all files already downloaded to this machine.
    No network call needed — purely reads the local download folder.
    """
    banner("My Downloaded Files")

    # List only actual files (not subdirectories) in DOWNLOAD_DIR.
    files = sorted(
        f for f in os.listdir(DOWNLOAD_DIR)
        if os.path.isfile(os.path.join(DOWNLOAD_DIR, f))
    )

    if not files:
        warn(f"No files in {DOWNLOAD_DIR}/")
        return

    for f in files:
        size = os.path.getsize(os.path.join(DOWNLOAD_DIR, f))
        # :<44 left-aligns filename in a 44-char column
        # :>10 right-aligns size in a 10-char column
        print(f"  {C.YELLOW}•{C.RESET}  {f:<44}  {C.DIM}{fmt_size(size):>10}{C.RESET}")
    print()


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN MENU  — the text shown to the user between commands
# ══════════════════════════════════════════════════════════════════════════════

# This is just a multi-line string (f-string) with ANSI colour codes embedded.
# It's defined at module level (outside any function) so it's only built once
# rather than being re-created every time the menu is displayed.
MENU = f"""
{C.BOLD}{C.BLUE}  ╔══════════════════════════════════════════════╗{C.RESET}
{C.BOLD}{C.BLUE}  ║   Synapse  ·  Client  v2.0                  ║{C.RESET}
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


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT  — the program starts here
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    # Make the globals writable from inside main().
    # Without 'global', assigning to _server_ip inside a function would create
    # a local variable instead of modifying the module-level one.
    global _server_ip, _server_port

    # ── Startup banner ──────────────────────────────────────────────────────
    print(f"\n{C.BOLD}{C.CYAN}  ╔══════════════════════════════╗")
    print(f"  ║   Synapse  v2.0              ║")
    print(f"  ╚══════════════════════════════╝{C.RESET}\n")

    # ── Ask for server address ──────────────────────────────────────────────
    # Show the default value in brackets so the user can just press Enter
    # to accept it without retyping it.
    ip_in   = input(f"{C.CYAN}  Server IP    [{DEFAULT_IP}]:   {C.RESET}").strip()
    port_in = input(f"{C.CYAN}  Server Port  [{DEFAULT_PORT}]:      {C.RESET}").strip()

    # Use whatever the user typed, or fall back to the default if blank.
    _server_ip   = ip_in   if ip_in                 else DEFAULT_IP
    # isdigit() returns True only if every character is 0–9 (no minus, no dot).
    _server_port = int(port_in) if port_in.isdigit() else DEFAULT_PORT

    # Attempt to connect (with auto-retry).
    sock = connect(_server_ip, _server_port)
    if not sock:
        # All retries failed — nothing more we can do.
        sys.exit(1)   # exit with code 1 to signal failure to the shell

    # ── Main interaction loop ────────────────────────────────────────────────
    while True:
        print(MENU)   # display the menu

        try:
            # Read the user's menu choice.
            choice = input(f"\n{C.CYAN}  › {C.RESET}").strip().lower()
        except (EOFError, KeyboardInterrupt):
            # Ctrl+D or Ctrl+C in the menu → quit gracefully.
            choice = "q"

        # Wrap every command in try/except so a single error doesn't crash
        # the whole program — the user can try again or choose a different option.
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
                # Send a clean disconnect notice so the server logs it properly.
                try:
                    send_msg(sock, "QUIT")
                    recv_msg(sock)   # receive the server's "BYE" acknowledgement
                except Exception:
                    pass             # if the socket is already dead, ignore the error
                ok("Disconnected. Goodbye!")
                break                # exit the while loop → fall through to cleanup
            elif choice == "":
                pass    # user pressed Enter with nothing — just re-show the menu
            else:
                warn(f"Unknown option '{choice}'")

        except ConnectionError as e:
            # The socket dropped during a command — try to reconnect.
            err(f"Connection lost: {e}")
            sock_new = reconnect(_server_ip, _server_port)
            if sock_new:
                sock = sock_new   # replace the dead socket with the new one
                ok("Reconnected! Continuing session.")
            else:
                err("Could not reconnect. Exiting.")
                break   # give up and exit

        except Exception as e:
            # Any other unexpected error — show it but don't crash.
            err(f"Unexpected error: {e}")
            # Loop continues so the user can try another option.

    # ── Cleanup ──────────────────────────────────────────────────────────────
    try:
        sock.close()   # release the socket and its OS resources
    except Exception:
        pass           # if it's already closed, that's fine


# Standard Python entry-point guard.
# __name__ == "__main__" is only True when this file is run directly
# (e.g.  python client.py).
# If someone imports this file as a module, __name__ will be "client"
# and main() won't be called automatically.
if __name__ == "__main__":
    main()