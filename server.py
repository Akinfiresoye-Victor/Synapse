# ═══════════════════════════════════════════════════════════════════════════════
#  server.py  —  Synapse  v2.0
#
#  What changed from the original (v1):
#  ──────────────────────────────────────
#  • TCP_NODELAY on every accepted socket  →  kills perceived lag instantly
#  • SO_SNDBUF / SO_RCVBUF bumped to 1 MB →  fewer kernel round-trips on big files
#  • SO_KEEPALIVE on every accepted socket →  dead clients detected automatically
#  • Broadcast chat  →  all connected clients receive each other's messages
#  • LIST_FILES command  →  client can query available files without downloading
#  • SERVER_INFO command →  returns uptime, client count, disk usage
#  • SEND_MULTI command  →  client can upload multiple files in one session
#  • Graceful Ctrl+C now closes ALL client sockets cleanly
#  • Structured shutdown event replaces bare KeyboardInterrupt hacks
#  • Length-prefixed message framing →  messages can NEVER get merged or split
#  • Removed blocking input() from chat →  server never freezes waiting for keyboard
# ═══════════════════════════════════════════════════════════════════════════════


# 'socket' is Python's built-in networking module.
# It lets us create TCP sockets — the foundation of all internet communication.
# TCP (Transmission Control Protocol) guarantees that data arrives in order
# and without corruption, unlike UDP which is "fire and forget".
import socket

# 'threading' lets the server handle multiple clients AT THE SAME TIME.
# Without this, client #2 would have to wait for client #1 to disconnect
# before the server could talk to them.  With threading, each client gets
# their own thread (a separate line of execution) running in parallel.
import threading

# 'hashlib' gives us SHA-256 — a cryptographic hash function.
# We use it to generate a "fingerprint" of every file before and after transfer.
# If both fingerprints match, the file arrived perfectly intact.
# If they differ, even by one byte, the file was corrupted and we delete it.
import hashlib

# 'os' is the operating system interface module.
# We use it for: creating directories, listing files, checking file sizes,
# building file paths, and deleting bad files.
import os

# 'logging' is Python's built-in event-logging system.
# Instead of plain print() calls, logging adds timestamps, severity levels
# (INFO / WARNING / ERROR), and automatically writes to log files.
# This is the professional way to track what a server is doing.
import logging

# 'time' gives us high-resolution timers.
# We use time.perf_counter() to measure how many seconds a file transfer takes,
# which lets us calculate and display the transfer speed in MB/s.
import time

# 'shutil' provides high-level file and directory operations.
# Here we use shutil.disk_usage() to check how much free disk space is left —
# part of the SERVER_INFO health report.
import shutil

# 'datetime' and 'timedelta' are for working with dates and times.
# datetime.now() gives us the current timestamp.
# timedelta lets us calculate how long the server has been running (uptime).
from datetime import datetime, timedelta


# ─── Logging setup ────────────────────────────────────────────────────────────

# Create the 'logs' directory if it does not already exist.
# exist_ok=True means: don't raise an error if the folder is already there.
# Without this, every server startup would crash if logs/ was missing.
os.makedirs("logs", exist_ok=True)

# Configure the global logging system for the entire server.
logging.basicConfig(
    # level=INFO means we capture: INFO, WARNING, ERROR, and CRITICAL messages.
    # DEBUG messages (very verbose) are silenced.
    level=logging.INFO,

    # This format string controls what each log line looks like.
    # %(asctime)s   → current date and time, e.g. "2025-05-10 14:30:22,123"
    # %(levelname)s → severity level, e.g. "INFO" or "WARNING"
    # %(message)s   → the actual log message text
    format="%(asctime)s [%(levelname)s] %(message)s",

    # 'handlers' is a list of destinations for log output.
    # We want logs to go to TWO places simultaneously:
    handlers=[
        # StreamHandler prints every log line to the terminal in real-time.
        # This lets the server operator watch activity as it happens.
        logging.StreamHandler(),

        # FileHandler writes every log line to a file on disk.
        # encoding="utf-8" ensures emojis and special characters don't corrupt the file.
        # The file grows forever — in production you'd rotate it daily.
        logging.FileHandler("logs/server.log", encoding="utf-8"),
    ],
)

# Create a logger object bound to this specific module.
# __name__ evaluates to "server" when this file is run directly.
# Using a named logger (instead of logging.info() directly) is best practice —
# it makes it easy to identify which module produced each log entry.
log = logging.getLogger(__name__)


# ─── Configuration ────────────────────────────────────────────────────────────

# socket.gethostname() returns this machine's network name (e.g. "VICTOR-PC").
# socket.gethostbyname() resolves that name to the actual LAN IP address
# (e.g. "192.168.1.5").  This is NOT 127.0.0.1 (loopback) — it's the real
# IP that other computers on the network can reach.
HOST_IP = socket.gethostbyname(socket.gethostname())

# The port number the server listens on.
# Ports below 1024 are "privileged" and require admin rights on Linux.
# 1234 is a safe, commonly used test port.
# This MUST match DEFAULT_PORT in client.py.
HOST_PORT = 1234

# All text sent over the network is encoded to bytes using UTF-8.
# UTF-8 can represent every character in every human language plus emojis.
# Both server and client must use the same encoding or messages get garbled.
ENCODER = "utf-8"

# The size of each data chunk read/written in one operation: 131072 bytes = 128 KB.
# This is a performance-critical setting:
#   Too small (e.g. 1024 bytes) → thousands of system calls per MB → slow
#   Too large (e.g. 10 MB)      → too much RAM used per client → wasteful
# 128 KB is a well-tested sweet spot for local network transfers.
BYTESIZE = 131072       # 128 KB chunks

# All uploaded files are stored in this directory.
# Keeping uploads separate makes them easy to manage and avoids cluttering
# the same folder where server.py lives.
UPLOAD_DIR = "server_files"

# If a client sends no commands for this many seconds, the server drops them.
# 300 seconds = 5 minutes.  This frees up resources from crashed/forgotten clients.
IDLE_TIMEOUT = 300

# Create the upload directory if it doesn't exist yet.
# Without this, the first file upload would crash with "No such file or directory".
os.makedirs(UPLOAD_DIR, exist_ok=True)


# ─── Server-wide shared state ──────────────────────────────────────────────────

# _clients is a dictionary that tracks every currently connected client.
# Key:   string like "192.168.1.7:52481"  (IP + port)
# Value: dict with keys "sock", "addr", "since", "name"
#
# Why track clients?
#   1. We need their sockets to broadcast chat messages to everyone.
#   2. We can count them for the SERVER_INFO command.
#   3. On shutdown, we can close all their sockets cleanly.
_clients: dict = {}

# A threading.Lock() is like a "take a number" ticket at the bank.
# When multiple threads try to modify _clients at the same time, only ONE
# thread is allowed in at a time — the others wait their turn.
#
# Without this lock, two threads could read _clients simultaneously,
# both see "5 clients", both try to add a 6th, and corrupt the dictionary.
# This class of bug is called a "race condition" — notoriously hard to debug.
_clients_lock = threading.Lock()

# threading.Event() is a shared on/off switch between threads.
# _shutdown_event.set()    → turns it ON  (signals all threads to stop)
# _shutdown_event.is_set() → True if ON
# _shutdown_event.clear()  → turns it OFF
# We use it to gracefully stop the server on Ctrl+C.
_shutdown_event = threading.Event()

# Record the exact moment the server process started.
# Used later to calculate uptime: current_time - _server_start = how long running.
_server_start = datetime.now()


# ─── Broadcast helper ─────────────────────────────────────────────────────────

def broadcast(sender_addr: str, message: str) -> None:
    """
    Send a chat message to EVERY connected client EXCEPT the sender.

    This is what makes group chat work — when Victor sends "hello",
    all other connected clients instantly receive it.

    Parameters:
      sender_addr — the IP:port string of the client who sent the message.
                    We exclude them so they don't receive their own message twice.
      message     — the formatted message string to send (already timestamped).
    """
    # We need to hold the lock to safely read _clients, but we don't want to
    # hold it while doing potentially slow network sends.  Solution: copy the
    # list of targets first (fast, lock held), then send to each (slow, no lock).
    with _clients_lock:
        # Build a list of (addr, socket) tuples for everyone except the sender.
        # Dictionary .items() returns (key, value) pairs.
        # We filter out the sender using: if addr != sender_addr
        targets = [
            (addr, info["sock"])
            for addr, info in _clients.items()
            if addr != sender_addr
        ]
    # Now the lock is released and other threads can modify _clients freely.

    # Send the message to each target client's socket.
    for addr, sock in targets:
        try:
            # send_msg() (defined below) handles the length-prefix framing.
            send_msg(sock, message)
        except Exception:
            # If a send fails (e.g. that client disconnected a moment ago),
            # silently ignore it.  Their own thread will clean them up shortly.
            pass


# ══════════════════════════════════════════════════════════════════════════════
#  LENGTH-PREFIXED PROTOCOL
#
#  THE PROBLEM WITH THE ORIGINAL CODE:
#  ─────────────────────────────────────
#  The original server used:   data = client_socket.recv(BYTESIZE)
#
#  TCP is a *stream* protocol, not a *message* protocol.  The OS delivers
#  data as a continuous flow of bytes, not as separate "messages".
#
#  When you call recv(65536), you might get:
#    a) Less than expected  → rest arrives in next recv() call
#    b) More than expected  → you accidentally consumed the start of the NEXT message
#    c) Exactly what you wanted  → lucky, but don't rely on it
#
#  This caused the original app's messages to get mixed up, causing crashes
#  and corrupted file transfers.
#
#  THE SOLUTION — LENGTH PREFIX:
#  ──────────────────────────────
#  Before sending every message, prepend exactly 4 bytes that encode the
#  message's length as a big-endian integer.
#
#  Sending "HELLO" (5 bytes):
#    Step 1: len("HELLO") = 5
#    Step 2: 5 as 4 big-endian bytes = [0x00, 0x00, 0x00, 0x05]
#    Step 3: send [0x00, 0x00, 0x00, 0x05, H, E, L, L, O]
#
#  Receiver:
#    Step 1: Read exactly 4 bytes → interpret as integer → get 5
#    Step 2: Read exactly 5 bytes → decode → get "HELLO"
#
#  No ambiguity.  No merging.  No splitting.
#  This protocol is IDENTICAL on both server and client — they must match exactly.
# ══════════════════════════════════════════════════════════════════════════════

def _recv_n(sock: socket.socket, n: int) -> bytes:
    """
    Read EXACTLY n bytes from the socket.  Blocks until all n bytes arrive.

    Why not just call sock.recv(n)?
      Because recv(n) means "give me UP TO n bytes".
      The OS might give you 50 when you asked for 100 — that is normal TCP behaviour.
      This function loops until it has collected the full n bytes you asked for.

    Parameters:
      sock — the connected TCP socket to read from
      n    — the exact number of bytes needed

    Returns:
      A bytes object of exactly length n.

    Raises:
      ConnectionError if the socket closes before n bytes arrive.
    """
    # bytearray is a mutable (editable) sequence of bytes.
    # We use it because we need to keep appending chunks to it.
    # (A plain bytes object is immutable and would require creating a new one each loop.)
    buf = bytearray()

    # Loop until we have collected exactly n bytes.
    while len(buf) < n:
        # How many bytes are we still missing?
        remaining = n - len(buf)

        # Ask the OS for data.
        # min(remaining, BYTESIZE) ensures we never ask for MORE than we need —
        # that could accidentally pull bytes belonging to the NEXT message.
        chunk = sock.recv(min(remaining, BYTESIZE))

        # recv() returning an empty bytes object b"" means the connection closed.
        # We raise an error because we cannot complete the read.
        if not chunk:
            raise ConnectionError("Socket closed unexpectedly")

        # Append the newly received data to our accumulator buffer.
        buf += chunk

    # Convert the mutable bytearray to an immutable bytes object.
    # This is the standard Python convention — functions return bytes, not bytearray.
    return bytes(buf)


def recv_msg(sock: socket.socket) -> str:
    """
    Receive one complete length-prefixed message and return it as a string.

    How it works:
      1. Read exactly 4 bytes.
      2. Convert those 4 bytes to an integer (the message length).
      3. Read exactly that many bytes.
      4. Decode from UTF-8 bytes to a Python string.
      5. Return the string.
    """
    # _recv_n(sock, 4) reads exactly 4 bytes (the length header).
    # int.from_bytes(..., "big") converts those 4 bytes to an integer.
    # "big" = big-endian byte order: the most significant byte comes first.
    # Example: bytes [0x00, 0x00, 0x00, 0x0C] → integer 12
    length = int.from_bytes(_recv_n(sock, 4), "big")

    # Now read exactly 'length' bytes and decode them to a Python string.
    # .decode(ENCODER) converts raw bytes → human-readable text using UTF-8.
    return _recv_n(sock, length).decode(ENCODER)


def send_msg(sock: socket.socket, text: str) -> None:
    """
    Send one complete length-prefixed message to the client.

    How it works:
      1. Encode the string to UTF-8 bytes.
      2. Get the byte count (the length).
      3. Convert that length to 4 big-endian bytes.
      4. Concatenate: [4-byte length] + [message bytes].
      5. sendall() sends every byte — never sends partial data.

    Why sendall() instead of send()?
      sock.send() might only send PART of the data if the kernel's send buffer
      is temporarily full.  sendall() loops internally and guarantees everything
      goes out, or raises an exception.  This is critical for correctness.
    """
    # .encode(ENCODER) converts the Python string to raw UTF-8 bytes.
    data = text.encode(ENCODER)

    # len(data) → integer byte count
    # .to_bytes(4, "big") → converts that integer to exactly 4 big-endian bytes
    # + data → concatenates the length header with the message payload
    # sendall() sends the entire combined byte string in one atomic call.
    sock.sendall(len(data).to_bytes(4, "big") + data)


# ─── File helper functions ────────────────────────────────────────────────────

def sha256_file(path: str) -> str:
    """
    Compute the SHA-256 hash of a file and return it as a 64-character hex string.

    SHA-256 is a cryptographic hash function.  Given any input (even a 10 GB file),
    it produces a fixed-size 256-bit (64 hex character) "fingerprint".
    The same input ALWAYS produces the same fingerprint.
    A single changed byte produces a completely different fingerprint.

    We use this to verify file integrity:
      Before send:  compute hash → call it H1
      After receive: compute hash of received data → call it H2
      If H1 == H2:  file is perfect ✓
      If H1 != H2:  something was corrupted → delete and try again ✗

    Why read in chunks instead of f.read() all at once?
      If the file is 2 GB and we call f.read(), Python loads 2 GB into RAM.
      By reading 128 KB at a time, we only use 128 KB of RAM regardless of size.
      The hash is computed incrementally — hashlib.sha256() supports this natively.
    """
    # Create a fresh SHA-256 hash object.  Think of it as an empty container
    # that we'll pour data into piece by piece.
    h = hashlib.sha256()

    # Open the file in binary read mode ('rb').
    # 'b' is critical — we want raw bytes, not text with newline translation.
    with open(path, "rb") as f:
        # iter(callable, sentinel) calls callable() repeatedly until it returns sentinel.
        # lambda: f.read(BYTESIZE) → reads 128 KB from the file each call
        # b""                      → stop when the file is exhausted (EOF)
        for chunk in iter(lambda: f.read(BYTESIZE), b""):
            # h.update(chunk) feeds this chunk into the running hash calculation.
            # It does NOT reset the hash — it ADDS to it incrementally.
            h.update(chunk)

    # hexdigest() finalises the calculation and returns a 64-char hex string.
    # Example: "a3f5b2c1d9e7f4a1b8c3d2e1f9a7b6c5d4e3f2a1b0c9d8e7f6a5b4c3d2e1f0a9"
    return h.hexdigest()


def unique_path(directory: str, filename: str) -> str:
    """
    Return a safe file path inside 'directory' for 'filename'.

    If a file with that name already exists in the directory, we append a
    timestamp so we NEVER silently overwrite an old upload.

    Example:
      First upload of "photo.jpg"  → "server_files/photo.jpg"
      Second upload of "photo.jpg" → "server_files/photo_20250510_143022.jpg"

    Why use os.path.basename(filename)?
      If a malicious client sends filename = "../../evil.sh", basename() strips
      the directory traversal and gives us just "evil.sh".  This prevents
      path traversal attacks where an attacker writes files outside UPLOAD_DIR.
    """
    # os.path.join() safely combines a directory path with a filename.
    # os.path.basename() ensures no directory components sneak through.
    target = os.path.join(directory, os.path.basename(filename))

    # If no file exists at this path yet, it is safe to use as-is.
    if not os.path.exists(target):
        return target

    # A file with this name already exists — we need to create a unique name.
    # os.path.splitext("photo.jpg") returns ("photo", ".jpg")
    # This splits the name from the extension so we can insert the timestamp between them.
    name, ext = os.path.splitext(os.path.basename(filename))

    # strftime formats the current time as a string.
    # "%Y%m%d_%H%M%S" → "20250510_143022" (year month day _ hour minute second)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Result: "server_files/photo_20250510_143022.jpg"
    return os.path.join(directory, f"{name}_{ts}{ext}")


def list_files() -> list:
    """
    Return a sorted list of filenames currently stored in UPLOAD_DIR.

    os.listdir() returns ALL items (files and subdirectories).
    We filter using os.path.isfile() to exclude any subdirectories.
    sorted() gives alphabetical ordering so the file list is predictable.
    """
    return sorted(
        f for f in os.listdir(UPLOAD_DIR)       # iterate every item in the directory
        if os.path.isfile(os.path.join(UPLOAD_DIR, f))  # keep only actual files
    )


def fmt_size(n: int) -> str:
    """
    Convert a raw byte count into a human-readable size string.

    Examples:
      500         → "500.0 B"
      2_097_152   → "2.0 MB"
      1_073_741_824 → "1.0 GB"

    Algorithm:
      Try each unit from smallest (B) to largest (GB).
      As long as n >= 1024, divide by 1024 and move up one unit.
      Stop when n < 1024 and format with that unit.
    """
    # Iterate through unit labels from smallest to largest.
    for unit in ("B", "KB", "MB", "GB"):
        # If the value fits in this unit (less than 1024), format and return.
        if n < 1024:
            # :.1f formats the number to 1 decimal place.
            return f"{n:.1f} {unit}"
        # Value doesn't fit yet — divide by 1024 to try the next unit up.
        n /= 1024
    # After GB, the only remaining unit is TB (terabytes).
    return f"{n:.1f} TB"


# ══════════════════════════════════════════════════════════════════════════════
#  COMMAND HANDLERS
#
#  Each function below handles one type of client request.
#  They are called from handle_client() when the client sends the matching
#  command string (e.g. "SEND_FILE", "GET_FILE", "CHAT", etc.).
#
#  All handlers share the same socket 'sock' and use send_msg/recv_msg
#  so the framing protocol is always respected.
# ══════════════════════════════════════════════════════════════════════════════

def h_send_file(sock: socket.socket, addr: str) -> None:
    """
    Handle a file UPLOAD from the client to the server.

    This is called when a client sends the "SEND_FILE" command.
    The server receives the file, verifies its integrity with SHA-256,
    and saves it to UPLOAD_DIR.

    Full protocol flow:
      Server → "READY"                           (I'm ready, send the file info)
      Client → filename    (e.g. "photo.jpg")
      Client → filesize    (e.g. "104857600")    (as a string)
      Client → sha256      (64-char hex string)  (expected fingerprint)
      Client → [raw file bytes, exactly filesize bytes]
      Server → "OK|saved_name|speed_in_MB_s"    (on success)
      Server → "FAIL|reason"                     (on failure)

    Parameters:
      sock — the client's connected socket
      addr — the client's IP:port string (used for logging)
    """
    # Tell the client we're ready to receive the file metadata.
    send_msg(sock, "READY")

    # Receive the three pieces of metadata the client sends before the raw data.
    filename      = recv_msg(sock)          # e.g. "photo.jpg"
    filesize      = int(recv_msg(sock))     # e.g. 104857600 (convert string → int)
    expected_hash = recv_msg(sock)          # 64-char SHA-256 hex string

    # Choose a safe destination path (adds timestamp if filename already exists).
    dest = unique_path(UPLOAD_DIR, filename)

    # Log the incoming upload: who it's from and how big the file is.
    # fmt_size() converts raw bytes to human-readable (e.g. "100.0 MB").
    log.info(f"[{addr}] ↑ receiving '{os.path.basename(dest)}'  ({fmt_size(filesize)})")

    received = 0              # bytes received so far (tracking progress)
    hasher   = hashlib.sha256()  # build the hash as we receive data chunk by chunk
    t0       = time.perf_counter()  # start timing the transfer for speed calculation

    try:
        # Open the destination file for writing in binary mode.
        # 'wb' = write binary.  The file is created if it doesn't exist.
        with open(dest, "wb") as f:
            # Keep receiving until we have collected every byte of the file.
            while received < filesize:
                # CRITICAL: min(BYTESIZE, filesize - received)
                # We only ask for as many bytes as we still need.
                # Without this, recv() might grab bytes from the NEXT protocol message.
                # Example: if 100 bytes are left but BYTESIZE is 128KB, we only ask for 100.
                chunk = sock.recv(min(BYTESIZE, filesize - received))

                # If recv() returns empty bytes, the client disconnected mid-upload.
                if not chunk:
                    raise ConnectionError("Client dropped mid-upload")

                # Write the received chunk directly to disk (streaming write).
                # This is memory-efficient — we never hold the whole file in RAM.
                f.write(chunk)

                # Feed this chunk into the running SHA-256 hash calculation.
                hasher.update(chunk)

                # Update our byte counter.
                received += len(chunk)

    except Exception as e:
        # Something went wrong (network error, disk full, client disconnect, etc.)
        # Clean up the partial file so we don't leave corrupted data on disk.
        if os.path.exists(dest):
            os.remove(dest)

        # Tell the client the upload failed and why.
        send_msg(sock, f"FAIL|Transfer interrupted: {e}")
        log.warning(f"[{addr}] ✗ upload aborted: {e}")
        return   # exit the function — no success message

    # Calculate how long the transfer took.
    # max(..., 1e-9) prevents division by zero if the transfer was instantaneous.
    elapsed = max(time.perf_counter() - t0, 1e-9)

    # Speed in MB/s: bytes ÷ seconds ÷ bytes_per_megabyte
    # 1_048_576 = 1024 × 1024 = bytes in 1 MB
    speed = received / elapsed / 1_048_576

    # Verify integrity:
    # hasher.hexdigest() = hash of what we actually received
    # expected_hash      = hash the client computed before sending
    # If they match AND the byte count matches → file is perfect.
    if hasher.hexdigest() == expected_hash and received == filesize:
        # Tell the client the upload succeeded.
        # Format: "OK|saved_filename|speed_in_MB_per_s"
        send_msg(sock, f"OK|{os.path.basename(dest)}|{speed:.2f}")
        log.info(f"[{addr}] ✓ saved '{os.path.basename(dest)}'  ({speed:.2f} MB/s)")
    else:
        # Hashes don't match → the file was corrupted during transfer.
        # Delete the bad file immediately — don't store corrupted data.
        os.remove(dest)
        send_msg(sock, "FAIL|Hash mismatch — file discarded")
        log.warning(f"[{addr}] ✗ hash mismatch for '{filename}'")


def h_send_multi(sock: socket.socket, addr: str) -> None:
    """
    Handle a BATCH of file uploads in one continuous session.

    Instead of reconnecting and re-handshaking for every file, the client
    sends multiple files back-to-back.  This is faster and more efficient
    for bulk transfers (e.g. uploading a whole project folder).

    Protocol:
      Server → "READY"
      Client → count (number of files as a string, e.g. "5")
      Repeat 'count' times:
        Client → filename
        Client → filesize (str)
        Client → sha256
        Client → [raw bytes × filesize]
        Server → "OK|saved_name|speed"  or  "FAIL|reason"
      Server → "MULTI_DONE|n_ok|n_fail"  (summary of the whole batch)

    Parameters:
      sock — the client's connected socket
      addr — the client's IP:port string (for logging)
    """
    # Tell the client we're ready to receive the batch.
    send_msg(sock, "READY")

    # How many files are in this batch?
    count = int(recv_msg(sock))   # client sends a number as a string, e.g. "5"
    log.info(f"[{addr}] ↑↑ batch upload: {count} file(s)")

    # Track results across all files in the batch.
    n_ok = n_fail = 0

    # Process each file one by one.
    # range(count) gives 0, 1, 2, …, count-1 — we use i for logging position.
    for i in range(count):
        # Receive this file's metadata (same three messages as single upload).
        filename      = recv_msg(sock)
        filesize      = int(recv_msg(sock))
        expected_hash = recv_msg(sock)

        # Get a unique save path for this file.
        dest    = unique_path(UPLOAD_DIR, filename)
        received = 0
        hasher   = hashlib.sha256()
        t0       = time.perf_counter()
        ok_flag  = True   # assume success unless an exception fires

        try:
            with open(dest, "wb") as f:
                while received < filesize:
                    # Same careful recv() as single-file upload — never overshoot.
                    chunk = sock.recv(min(BYTESIZE, filesize - received))
                    if not chunk:
                        raise ConnectionError("Client dropped mid-batch")
                    f.write(chunk)
                    hasher.update(chunk)
                    received += len(chunk)
        except Exception as e:
            # Clean up partial file on error.
            if os.path.exists(dest):
                os.remove(dest)
            # Send a FAIL response for this individual file in the batch.
            send_msg(sock, f"FAIL|{e}")
            ok_flag = False   # mark this file as failed

        if ok_flag:
            # File was received without exceptions — now verify integrity.
            elapsed = max(time.perf_counter() - t0, 1e-9)
            speed   = received / elapsed / 1_048_576

            if hasher.hexdigest() == expected_hash and received == filesize:
                # Send success response for this file.
                send_msg(sock, f"OK|{os.path.basename(dest)}|{speed:.2f}")
                log.info(f"[{addr}] ✓ batch {i+1}/{count} '{os.path.basename(dest)}'  ({speed:.2f} MB/s)")
                n_ok += 1   # increment success counter
            else:
                # Hash mismatch → delete the corrupted file.
                os.remove(dest)
                send_msg(sock, "FAIL|Hash mismatch")
                n_fail += 1   # increment failure counter

    # After all files, send a summary to the client.
    # Format: "MULTI_DONE|n_successful|n_failed"
    send_msg(sock, f"MULTI_DONE|{n_ok}|{n_fail}")
    log.info(f"[{addr}] ↑↑ batch done: {n_ok} ok, {n_fail} failed")


def h_get_file(sock: socket.socket, addr: str) -> None:
    """
    Handle a file DOWNLOAD from the server to the client.

    This is called when a client sends the "GET_FILE" command.
    The server lists available files, the client picks one, and we send it.

    Full protocol flow:
      Server → "NOFILES"                      (if nothing to download — done)
      Server → "FILES|file1|file2|…"          (pipe-separated list of filenames)
      Client → choice (1-based number)  |  "CANCEL"
      Server → "ERROR|reason"                 (if choice was invalid)
      Server → "META|name|size|sha256"        (file info before downloading)
      Client → "ACK"  |  "NACK"              (proceed or cancel)
      Server → [raw bytes × size]             (file data, only sent after ACK)

    Parameters:
      sock — the client's connected socket
      addr — the client's IP:port string (for logging)
    """
    # Get the list of files currently available for download.
    files = list_files()

    # If there are no files, tell the client immediately and return.
    if not files:
        send_msg(sock, "NOFILES")
        return

    # Send the file list as a pipe-separated string.
    # "|".join(files) converts ["file1.jpg", "doc.pdf"] → "file1.jpg|doc.pdf"
    # The client receives "FILES|file1.jpg|doc.pdf" and splits on "|".
    send_msg(sock, "FILES|" + "|".join(files))

    # Wait for the client's choice (a number like "2" or the word "CANCEL").
    choice = recv_msg(sock)

    # Client decided not to download anything.
    if choice == "CANCEL":
        return

    # Validate the client's choice.
    try:
        # Convert the string "2" to integer index 1 (0-based: choice - 1).
        idx = int(choice) - 1
        # assert raises AssertionError if the condition is False.
        # This checks the index is within the valid range of the files list.
        assert 0 <= idx < len(files)
    except (ValueError, AssertionError):
        # ValueError  → "abc" is not a valid integer
        # AssertionError → index out of range (e.g. client said "99" but only 3 files)
        send_msg(sock, "ERROR|Invalid choice")
        return

    # Build the full path to the requested file.
    path     = os.path.join(UPLOAD_DIR, files[idx])

    # os.path.getsize() returns the file's byte count without opening it.
    filesize = os.path.getsize(path)

    # Compute SHA-256 of the file so the client can verify the download.
    fhash    = sha256_file(path)

    # Send the file's metadata to the client so they can decide whether to proceed.
    # Format: "META|filename|bytecount|sha256"
    send_msg(sock, f"META|{files[idx]}|{filesize}|{fhash}")

    # Wait for the client's confirmation: "ACK" = proceed, anything else = cancel.
    ack = recv_msg(sock)
    if ack != "ACK":
        log.info(f"[{addr}] download cancelled after META")
        return

    log.info(f"[{addr}] ↓ sending '{files[idx]}'  ({fmt_size(filesize)})")

    # Start timing the transfer for speed calculation.
    t0   = time.perf_counter()
    sent = 0   # bytes sent so far

    # Send the file contents in BYTESIZE chunks.
    with open(path, "rb") as f:
        # iter(lambda: f.read(BYTESIZE), b"") reads 128 KB at a time until EOF.
        for chunk in iter(lambda: f.read(BYTESIZE), b""):
            # sendall() guarantees the entire chunk is sent.
            sock.sendall(chunk)
            sent += len(chunk)   # track total bytes sent

    # Calculate and log the final transfer speed.
    elapsed = max(time.perf_counter() - t0, 1e-9)
    speed   = sent / elapsed / 1_048_576
    log.info(f"[{addr}] ✓ sent '{files[idx]}'  ({speed:.2f} MB/s)")


def h_list_files(sock: socket.socket) -> None:
    """
    Handle a LIST_FILES command — show available files WITHOUT downloading.

    This is like a "peek at the menu before ordering".  The client can see
    what's available and their sizes, then decide whether to actually download.

    Protocol:
      Server → "NOFILES"
      Server → "FILELIST|name1:size1|name2:size2|…"

    Note: sizes are raw byte counts, the client formats them for display.
    """
    files = list_files()

    if not files:
        send_msg(sock, "NOFILES")
        return

    # Build a list of "filename:bytecount" strings, one per file.
    parts = []
    for f in files:
        # os.path.getsize() returns the byte count as an integer.
        sz = os.path.getsize(os.path.join(UPLOAD_DIR, f))
        parts.append(f"{f}:{sz}")   # e.g. "photo.jpg:104857600"

    # Join all entries with "|" and send them.
    # Result: "FILELIST|photo.jpg:104857600|doc.pdf:512000"
    send_msg(sock, "FILELIST|" + "|".join(parts))


def h_chat(sock: socket.socket, addr: str) -> None:
    """
    Handle a group CHAT session for this client.

    THE ORIGINAL PROBLEM:
      The old server called input() inside the chat handler:
        response = input("Server: ")
      input() BLOCKS — the entire thread freezes waiting for the server
      operator to type something.  During this freeze:
        • The client can't send any more messages
        • The client can't cancel (can't even type "quit")
        • Every OTHER client trying to interact is also blocked
      This made the chat completely broken in multi-client mode.

    THE FIX:
      The server no longer types replies.  Instead, it:
        1. Echoes every message back to the sender with a timestamp.
        2. Broadcasts the message to ALL other connected clients.
      The server operator sees chat in the log file.
      The result is a working GROUP CHAT — all clients see all messages.

    Protocol:
      Server → "CHAT_START"
      loop:
        Client → message text  |  "CHAT_QUIT"  (client wants to leave)
        Server → "[HH:MM:SS] display_name: message"  (echo back + broadcast)
      Server → "CHAT_END"

    Parameters:
      sock — the client's connected socket
      addr — the client's IP:port string
    """
    # Look up this client's display name (set via SET_NAME command, or defaults to addr).
    # We access _clients inside a lock because other threads might modify it.
    with _clients_lock:
        display_name = _clients.get(addr, {}).get("name", addr)

    # Tell the client they are now in chat mode.
    send_msg(sock, "CHAT_START")

    # Chat loop — runs until the client sends "CHAT_QUIT" or the connection breaks.
    while True:
        try:
            # Block here waiting for the client's next message.
            msg = recv_msg(sock)
        except Exception:
            # Any socket error (disconnect, timeout, etc.) → exit chat cleanly.
            break

        # Client wants to leave the chat.
        if msg == "CHAT_QUIT":
            break

        # Format the message with a timestamp and the sender's name.
        # strftime("%H:%M:%S") formats time as "14:30:22" (hour:minute:second).
        ts        = datetime.now().strftime("%H:%M:%S")
        formatted = f"[{ts}] {display_name}: {msg}"

        # Log the chat message on the server console / log file.
        log.info(f"[{addr}] 💬  {msg}")

        # Echo the formatted message back to the sender.
        send_msg(sock, formatted)

        # Broadcast the same message to ALL other connected clients.
        # This is what makes it a GROUP chat instead of a private echo.
        broadcast(addr, formatted)

    # Notify the client that the chat session has ended.
    send_msg(sock, "CHAT_END")


def h_server_info(sock: socket.socket) -> None:
    """
    Handle a SERVER_INFO command — return a snapshot of server health.

    Clients can use this to check:
      • How long the server has been running (uptime)
      • How many clients are currently connected
      • How many files are stored on the server
      • How much free disk space remains

    Protocol:
      Server → "INFO|uptime|client_count|file_count|disk_free_MB"

    All values are strings separated by "|" for easy parsing on the client side.
    """
    # timedelta calculates the difference between two datetime objects.
    # datetime.now() - _server_start = how long ago the server started.
    # .total_seconds() converts that to a float (e.g. 3661.5 seconds).
    # int() rounds it to a whole number.
    # str(timedelta(...)) formats it as "HH:MM:SS" (e.g. "1:01:01").
    uptime = str(timedelta(seconds=int((datetime.now() - _server_start).total_seconds())))

    # Count how many clients are currently connected.
    # Lock is required because another thread might be modifying _clients right now.
    with _clients_lock:
        n_clients = len(_clients)

    # Count files and check disk space.
    n_files    = len(list_files())

    # shutil.disk_usage(path) returns a named tuple with .total, .used, and .free.
    # .free gives us free bytes; dividing by 1_048_576 converts to MB.
    free_bytes = shutil.disk_usage(UPLOAD_DIR).free
    free_mb    = free_bytes / 1_048_576

    # Send all info as one pipe-separated message.
    # {free_mb:.0f} formats free_mb with 0 decimal places (e.g. "45231").
    send_msg(sock, f"INFO|{uptime}|{n_clients}|{n_files}|{free_mb:.0f}")


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN CLIENT HANDLER
#
#  This function is the "brain" of each client connection.
#  It runs in its own thread — one thread per connected client.
#
#  Flow:
#    1. Configure the socket for low latency (TCP_NODELAY etc.)
#    2. Register the client in _clients
#    3. Send a WELCOME message
#    4. Enter a command loop: receive command → dispatch to handler → repeat
#    5. On exit: remove from _clients, close socket
# ══════════════════════════════════════════════════════════════════════════════

def handle_client(sock: socket.socket, address: tuple) -> None:
    """
    Handle one client connection from start to finish.

    This function runs in a dedicated thread for each client.
    It configures socket options, then loops waiting for commands.

    SOCKET OPTIONS EXPLAINED:
    ─────────────────────────
    TCP_NODELAY (most important for lag):
      Nagle's algorithm is a TCP feature that DELAYS small outgoing packets,
      hoping to batch them with more data into one bigger packet (saves bandwidth).
      The problem: for our command/response protocol this adds ~200ms of lag.
      Example without TCP_NODELAY:
        Client sends "PING" (4 bytes) → TCP holds it, waits for more data
        200ms later: "Okay fine, sending it" → server responds → 200ms delay
      Example with TCP_NODELAY:
        Client sends "PING" → TCP sends it IMMEDIATELY → fast response

    SO_SNDBUF / SO_RCVBUF (important for large files):
      The kernel keeps internal queues for outgoing and incoming data.
      If these queues are small (default ~64-256 KB), large file transfers
      have to pause frequently, waiting for the queue to drain.
      Bumping both to 1 MB (1 << 20 bytes) lets data flow more continuously.
      1 << 20 means "shift the number 1 left by 20 binary places" = 2^20 = 1,048,576.

    SO_KEEPALIVE (important for detecting dead clients):
      If a client's machine suddenly loses power or the network cable is pulled,
      there is no TCP FIN packet — the connection just silently "dies".
      Without keepalive, the server would wait FOREVER for data from that client.
      With SO_KEEPALIVE, the OS periodically sends a tiny heartbeat probe.
      If the probe gets no reply after several attempts, the OS closes the socket.

    Parameters:
      sock    — the accepted client socket (from server_socket.accept())
      address — a tuple (ip_string, port_int), e.g. ("192.168.1.7", 52481)
    """
    # ── Apply socket performance options ──────────────────────────────────────

    # IPPROTO_TCP tells setsockopt() this option applies to the TCP layer.
    # TCP_NODELAY = 1 turns OFF Nagle's algorithm → immediate packet sending.
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY,  1)

    # SOL_SOCKET tells setsockopt() this option applies to the socket layer.
    # SO_SNDBUF sets the send buffer size to 1 MB.
    sock.setsockopt(socket.SOL_SOCKET,  socket.SO_SNDBUF,    1 << 20)

    # SO_RCVBUF sets the receive buffer size to 1 MB.
    sock.setsockopt(socket.SOL_SOCKET,  socket.SO_RCVBUF,    1 << 20)

    # SO_KEEPALIVE = 1 enables automatic dead-client detection.
    sock.setsockopt(socket.SOL_SOCKET,  socket.SO_KEEPALIVE, 1)

    # settimeout(IDLE_TIMEOUT) means: if we don't receive a command within
    # IDLE_TIMEOUT seconds (300 = 5 minutes), recv_msg() raises socket.timeout.
    # This prevents idle zombie connections from occupying server resources forever.
    sock.settimeout(IDLE_TIMEOUT)

    # Build the string representation of this client's address for logging.
    # address[0] = IP string,  address[1] = port integer
    addr = f"{address[0]}:{address[1]}"

    # Register this client in the global client registry.
    # We lock _clients to prevent race conditions with other threads.
    with _clients_lock:
        _clients[addr] = {
            "sock"  : sock,          # socket for broadcasting chat messages
            "addr"  : address,       # raw (ip, port) tuple
            "since" : datetime.now(), # when this client connected
            "name"  : addr,          # display name (defaults to IP:port)
        }
        # Count clients INSIDE the lock so it's an accurate snapshot.
        n = len(_clients)

    log.info(f"✚  {addr} connected   (clients online: {n})")

    try:
        # Send the WELCOME message immediately after accepting.
        # The client expects this as the first thing it receives.
        # Format: "WELCOME|app_name|server_address|client_count"
        send_msg(sock, f"WELCOME|Synapse v2|{HOST_IP}:{HOST_PORT}|{n}")

        # ── Command loop ───────────────────────────────────────────────────────
        # Keep processing commands until the client quits or an error occurs.
        while not _shutdown_event.is_set():
            try:
                # Block here, waiting for the next command from this client.
                # If no command arrives within IDLE_TIMEOUT seconds, socket.timeout fires.
                cmd = recv_msg(sock)
            except socket.timeout:
                # Client was idle for too long — drop the connection.
                log.warning(f"[{addr}] idle for {IDLE_TIMEOUT}s — dropping")
                break   # exit the while loop → goes to finally block

            # ── Route the command to the correct handler ───────────────────────

            if cmd == "QUIT":
                # Client is disconnecting gracefully.
                send_msg(sock, "BYE")
                break   # exit the loop

            elif cmd == "PING":
                # Simple round-trip latency check.
                # Client measures the time between sending PING and receiving PONG.
                send_msg(sock, "PONG")

            elif cmd == "SEND_FILE":
                # Client wants to upload one file.
                h_send_file(sock, addr)

            elif cmd == "SEND_MULTI":
                # Client wants to upload multiple files in one batch.
                h_send_multi(sock, addr)

            elif cmd == "GET_FILE":
                # Client wants to download a file from the server.
                h_get_file(sock, addr)

            elif cmd == "LIST_FILES":
                # Client wants to see what files are available (no download).
                h_list_files(sock)

            elif cmd == "CHAT":
                # Client wants to enter group chat mode.
                h_chat(sock, addr)

            elif cmd == "SERVER_INFO":
                # Client wants the server's health statistics.
                h_server_info(sock)

            elif cmd.startswith("SET_NAME|"):
                # Client wants to set their chat display name.
                # cmd.split("|", 1) splits at the FIRST "|" only, giving ["SET_NAME", "Victor"].
                # [1] gets the name part.  [:32] limits it to 32 characters max.
                new_name = cmd.split("|", 1)[1][:32].strip()
                with _clients_lock:
                    if addr in _clients:
                        _clients[addr]["name"] = new_name
                send_msg(sock, f"NAME_OK|{new_name}")
                log.info(f"[{addr}] renamed to '{new_name}'")

            else:
                # Unknown command — tell the client and continue the loop.
                # We don't disconnect for unknown commands — resilient design.
                send_msg(sock, f"ERROR|Unknown command '{cmd}'")

    except ConnectionError:
        # The client's connection dropped unexpectedly (network issue, crash, etc.).
        # This is not a bug — log it as INFO, not ERROR.
        log.info(f"[{addr}] connection dropped by client")

    except Exception as e:
        # Any other unexpected exception in the command loop.
        # exc_info=True tells the logger to include the full traceback.
        log.error(f"[{addr}] unhandled error: {e}", exc_info=True)

    finally:
        # This block ALWAYS runs, whether we exited normally or via an exception.
        # It ensures we always clean up, no matter how we got here.

        # Remove this client from the registry so they're no longer counted
        # and no longer receive broadcasts.
        # .pop(addr, None) removes the key if it exists; returns None if not found.
        # Using None as default prevents a KeyError if the key was already removed.
        with _clients_lock:
            _clients.pop(addr, None)

        # Close the socket to free the OS resources (file descriptor, port, etc.).
        try:
            sock.close()
        except Exception:
            pass   # Already closed — that's fine

        log.info(f"✖  {addr} disconnected  (clients online: {len(_clients)})")


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT  —  the program starts here
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    """
    Create the server socket, bind it to the network, and accept clients forever.

    This function runs in the MAIN thread.  Every client that connects gets
    its own daemon thread (via handle_client) so they can all be served in parallel.
    """
    # Create the server socket.
    # AF_INET      = IPv4 addressing (the standard internet protocol)
    # SOCK_STREAM  = TCP — reliable, ordered, connection-based
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # SO_REUSEADDR is CRITICAL for development.
    # When you stop and restart the server quickly, the OS keeps the port in
    # "TIME_WAIT" state for ~30-120 seconds (it's waiting to catch any stray packets).
    # Without SO_REUSEADDR, restarting gives: "OSError: [Errno 98] Address already in use"
    # With SO_REUSEADDR, the server can immediately re-bind the same port.
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # bind() tells the OS: "this socket should receive connections on this IP and port".
    # (HOST_IP, HOST_PORT) is a tuple — Python requires parentheses here.
    srv.bind((HOST_IP, HOST_PORT))

    # listen(50) puts the socket into "listening" mode — now it can accept connections.
    # The argument 50 is the "backlog" — how many pending connection requests
    # the OS should queue up if we're busy processing a connection.
    # If the queue fills up, new clients get a "connection refused" error.
    srv.listen(50)

    # Log a startup banner showing the server's key configuration.
    log.info("=" * 56)
    log.info("   Synapse  Server  v2.0")
    log.info(f"   Listening  :  {HOST_IP}:{HOST_PORT}")
    log.info(f"   Files dir  :  {os.path.abspath(UPLOAD_DIR)}")
    log.info(f"   Chunk size :  {BYTESIZE // 1024} KB")
    log.info(f"   Idle limit :  {IDLE_TIMEOUT}s")
    log.info("=" * 56)

    try:
        # Main accept loop — runs forever (until Ctrl+C or _shutdown_event is set).
        while not _shutdown_event.is_set():
            try:
                # Set a 1-second timeout on the server socket.
                # This means accept() will return (with a timeout exception) every second
                # instead of blocking forever.  Without this, Ctrl+C during a quiet period
                # (no incoming connections) would never interrupt the accept() call.
                srv.settimeout(1.0)

                try:
                    # accept() blocks until a client connects.
                    # Returns (client_socket, client_address).
                    # client_socket is a NEW socket just for this client.
                    # The server socket (srv) stays open to accept more clients.
                    cli_sock, cli_addr = srv.accept()
                except socket.timeout:
                    # No connection arrived in 1 second — loop back and check _shutdown_event.
                    continue

                # Spawn a new thread to handle this client.
                # target=handle_client means that function runs in the new thread.
                # args=(cli_sock, cli_addr) passes the client socket and address to it.
                # daemon=True means: when the main thread exits (Ctrl+C), this thread
                # is killed automatically instead of keeping the program alive.
                threading.Thread(
                    target=handle_client,
                    args=(cli_sock, cli_addr),
                    daemon=True,
                ).start()

            except OSError:
                # srv.accept() raised an OSError — this usually means srv was closed.
                # Break out of the accept loop.
                break

    except KeyboardInterrupt:
        # User pressed Ctrl+C — signal all threads to stop.
        log.info("Ctrl+C — shutting down…")
        _shutdown_event.set()   # flip the shutdown switch

    finally:
        # Always run this block, even if an exception occurred.

        # Close all active client sockets.
        # This causes their recv_msg() calls to raise ConnectionError,
        # which makes their threads exit the command loop and clean up.
        with _clients_lock:
            for info in _clients.values():
                try:
                    info["sock"].close()
                except Exception:
                    pass   # already closed — ignore

        # Close the main server socket.
        srv.close()
        log.info("Server stopped.")


# Standard Python entry-point guard.
# __name__ == "__main__" is True when this file is executed directly:
#   python server.py         →  __name__ is "__main__"  →  main() is called
#
# If someone does "import server" in another file:
#   import server            →  __name__ is "server"    →  main() is NOT called
# This prevents the server from auto-starting when imported as a module.
if __name__ == "__main__":
    main()
