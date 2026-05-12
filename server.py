import socket
import threading
import hashlib
import os
import logging
import time
import shutil
from datetime import datetime, timedelta


# ─── Logging Setup ────────────────────────────────────────────────────────────
# Create a logs folder if it doesn't exist
os.makedirs("logs", exist_ok=True)

# This sets up logging to print to the terminal AND save to a file at the same time
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),                                  # prints to terminal
        logging.FileHandler("logs/server.log", encoding="utf-8") # saves to file
    ]
)

# 'log' is our logger object — we use log.info(), log.warning(), log.error()
log = logging.getLogger(__name__)


# ─── Config ───────────────────────────────────────────────────────────────────
# Get this machine's actual LAN IP (not 127.0.0.1)
HOST_IP   = socket.gethostbyname(socket.gethostname())
HOST_PORT = 1234          # port the server listens on — must match client
ENCODER   = "utf-8"       # text encoding used for all messages
BYTESIZE  = 131072        # 128 KB — how many bytes we read/send at a time
UPLOAD_DIR = "server_files"  # folder where uploaded files are stored
IDLE_TIMEOUT = 300        # drop a client if they do nothing for 5 minutes

# Create the upload folder if it doesn't exist
os.makedirs(UPLOAD_DIR, exist_ok=True)


# ─── Shared State ─────────────────────────────────────────────────────────────
# Dictionary that keeps track of every connected client
# Key = "ip:port" string, Value = dict with their socket, address, name, etc.
_clients = {}

# A Lock prevents two threads from editing _clients at the same time
# Think of it as a "one at a time" door — only one thread can enter at once
_clients_lock = threading.Lock()

# This event is used to signal all threads to stop when the server shuts down
_shutdown_event = threading.Event()
# Record when the server started so we can calculate uptime later
_server_start = datetime.now()


# ─── Broadcast ────────────────────────────────────────────────────────────────
def broadcast(sender_addr, message):
    # Send a chat message to everyone except the person who sent it
    # First, grab a copy of the targets while holding the lock
    with _clients_lock:
        targets = [
            (addr, info["sock"])
            for addr, info in _clients.items()
            if addr != sender_addr  # skip the sender
        ]
    # Now send to each target (we released the lock so other threads can run)
    for addr, sock in targets:
        try:
            send_msg(sock, message)
        except Exception:
            pass  # if that client disconnected, ignore the error




def read_exactly(sock, n):
    # Read exactly n bytes from the socket, looping until we have them all
    buf = bytearray()  # bytearray is like a list of bytes we can keep adding to
    while len(buf) < n:
        remaining = n - len(buf)
        chunk = sock.recv(min(remaining, BYTESIZE))  # ask for what we still need
        if not chunk:
            # recv returns empty bytes when the connection is closed
            raise ConnectionError("Connection closed before all bytes arrived")
        buf += chunk
    return bytes(buf)


def recv_msg(sock):
    # Step 1: read the 4-byte length header
    length = int.from_bytes(read_exactly(sock, 4), "big")
    # Step 2: read exactly that many bytes and decode to string
    return read_exactly(sock, length).decode(ENCODER)


def send_msg(sock, text):
    # Encode the string to bytes to make it streamable
    data = text.encode(ENCODER)
    # Stick the 4-byte length in front, then send everything at once
    # sendall() makes sure every byte is sent (send() might send only part)
    sock.sendall(len(data).to_bytes(4, "big") + data)


# ─── File Helpers ─────────────────────────────────────────────────────────────
#Integrity Check 
def sha256_file(path):
    # Compute the SHA-256 hash (fingerprint) of a file
    # We read in chunks so we don't load the whole file into RAM at once
    h = hashlib.sha256()
    with open(path, "rb") as f:
        #Algorithm to reduce RAM/Memory Consumption
        while True:
            chunk = f.read(BYTESIZE)
            if not chunk:
                break  # end of file
            h.update(chunk)  # feed the chunk into the hash
    return h.hexdigest()  # returns a 64-character hex string

# Settin gpath to save files to the server with Security Checks
def unique_path(directory, filename):
    # Build a file path that won't overwrite an existing file
    # os.path.basename() strips any folder part from the filename
    # This also prevents path traversal attacks like "../../evil.sh"
    safe_name = os.path.basename(filename)
    dest = os.path.join(directory, safe_name)

    if not os.path.exists(dest):
        return dest  # path is free, use it as is

    # File already exists — add a timestamp to make the name unique
    name, ext = os.path.splitext(safe_name)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(directory, f"{name}_{timestamp}{ext}")


def list_files():
    # Return a sorted list of files currently stored on the server
    files = []
    for f in os.listdir(UPLOAD_DIR):
        full_path = os.path.join(UPLOAD_DIR, f)
        if os.path.isfile(full_path):
            files.append(f)
    return sorted(files)


def fmt_size(n):
    # Convert a byte count to a human readable string like "2.0 MB"
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


# ─── Command Handlers ─────────────────────────────────────────────────────────

def h_send_file(sock, addr):
    # Handle a single file upload from the client
    # Protocol:
    #   Server → "READY"
    #   Client → filename, filesize (as string), sha256 hash
    #   Client → raw file bytes
    #   Server → "OK|saved_name|speed" or "FAIL|reason"

    send_msg(sock, "READY")  # tell client we're ready to receive

#Dont ask how im still confused but it works
    filename      = recv_msg(sock)
    filesize      = int(recv_msg(sock))   # client sends the size as text like "1048576"
    expected_hash = recv_msg(sock)        # 64-char hex SHA-256 string


    dest = unique_path(UPLOAD_DIR, filename)
    log.info(f"[{addr}] receiving '{os.path.basename(dest)}' ({fmt_size(filesize)})")

    received = 0
    hasher   = hashlib.sha256()   # we'll build the hash as we receive chunks
    t0       = time.perf_counter()

    try:
        with open(dest, "wb") as f:
            while received < filesize:
                # Only ask for as many bytes as we still need
                # Without this, we might accidentally read bytes from the next message
                to_read = min(BYTESIZE, filesize - received)
                chunk   = sock.recv(to_read)

                if not chunk:
                    raise ConnectionError("Client disconnected mid-upload")

                f.write(chunk)
                hasher.update(chunk)
                received += len(chunk)

    except Exception as e:
        # Something went wrong — delete the partial file
        if os.path.exists(dest):
            os.remove(dest)
        send_msg(sock, f"FAIL|Upload failed: {e}")
        log.warning(f"[{addr}] upload failed: {e}")
        return

    # Calculate how fast the transfer was
    elapsed = max(time.perf_counter() - t0, 1e-9)  # avoid dividing by zero
    speed   = received / elapsed / 1_048_576         # convert to MB/s

    # Check if the file arrived intact by comparing hashes
    if hasher.hexdigest() == expected_hash and received == filesize:
        send_msg(sock, f"OK|{os.path.basename(dest)}|{speed:.2f}")
        log.info(f"[{addr}] saved '{os.path.basename(dest)}' at {speed:.2f} MB/s")
    else:
        # Hash mismatch means the file was corrupted — delete it
        os.remove(dest)
        send_msg(sock, "FAIL|File was corrupted (hash mismatch)")
        log.warning(f"[{addr}] hash mismatch for '{filename}'")


def h_send_multi(sock, addr):
    # Handle a batch upload — multiple files sent one after another
    # Protocol:
    #   Server → "READY"
    #   Client → total count of files (as string)
    #   Then for each file, same steps as h_send_file
    #   Server → "MULTI_DONE|ok_count|fail_count"

    send_msg(sock, "READY")
    count = int(recv_msg(sock))
    log.info(f"[{addr}] batch upload: {count} file(s)")

    n_ok   = 0
    n_fail = 0
#FIXME Put a folder transfer inside a folder not just as files
    for i in range(count):
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

        dest     = unique_path(UPLOAD_DIR, filename)
        received = 0
        hasher   = hashlib.sha256()
        t0       = time.perf_counter()
        success  = True

        try:
            with open(dest, "wb") as f:
                while received < filesize:
                    to_read = min(BYTESIZE, filesize - received)
                    chunk   = sock.recv(to_read)
                    if not chunk:
                        raise ConnectionError("Client disconnected mid-batch")
                    f.write(chunk)
                    hasher.update(chunk)
                    received += len(chunk)
        except Exception as e:
            if os.path.exists(dest):
                os.remove(dest)
            send_msg(sock, f"FAIL|{e}")
            success = False

        if success:
            elapsed = max(time.perf_counter() - t0, 1e-9)
            speed   = received / elapsed / 1_048_576

            if hasher.hexdigest() == expected_hash and received == filesize:
                send_msg(sock, f"OK|{os.path.basename(dest)}|{speed:.2f}")
                log.info(f"[{addr}] batch [{i+1}/{count}] '{os.path.basename(dest)}' OK")
                n_ok += 1
            else:
                os.remove(dest)
                send_msg(sock, "FAIL|Hash mismatch")
                n_fail += 1

    send_msg(sock, f"MULTI_DONE|{n_ok}|{n_fail}")
    log.info(f"[{addr}] batch done: {n_ok} ok, {n_fail} failed")


def h_get_file(sock, addr):
    # Handle a file download request from the client
    # Protocol:
    #   Server → "NOFILES" (if nothing stored) OR "FILES|file1|file2|..."
    #   Client → file number (1-based) OR "CANCEL"
    #   Server → "META|name|size|sha256" (info about the file)
    #   Client → "ACK" (go ahead) OR anything else (cancel)
    #   Server → raw file bytes

    files = list_files()

    if not files:
        send_msg(sock, "NOFILES")
        return

    # Send all filenames joined by "|"
    #Lists All the filename the client possesses
    send_msg(sock, "FILES|" + "|".join(files))

    choice = recv_msg(sock)

    if choice == "CANCEL":
        return

    # Validate the choice — it should be a number within range
    try:
        index = int(choice) - 1   # convert "2" → index 1 (0-based)
        if index < 0 or index >= len(files):
            raise ValueError("Out of range")
    except ValueError:
        send_msg(sock, "ERROR|Invalid choice")
        return

    chosen_file = files[index]
    path        = os.path.join(UPLOAD_DIR, chosen_file)
    filesize    = os.path.getsize(path)
    file_hash   = sha256_file(path)

    # Send file metadata so the client can decide whether to download
    send_msg(sock, f"META|{chosen_file}|{filesize}|{file_hash}")

    # Wait for the client to confirm
    ack = recv_msg(sock)
    if ack != "ACK":
        log.info(f"[{addr}] download cancelled")
        return

    log.info(f"[{addr}] sending '{chosen_file}' ({fmt_size(filesize)})")

    t0   = time.perf_counter()
    sent = 0

    with open(path, "rb") as f:
        while True:
            chunk = f.read(BYTESIZE)
            if not chunk:
                break  # end of file
            sock.sendall(chunk)
            sent += len(chunk)

    elapsed = max(time.perf_counter() - t0, 1e-9)
    speed   = sent / elapsed / 1_048_576
    log.info(f"[{addr}] sent '{chosen_file}' at {speed:.2f} MB/s")


def h_list_files(sock):
    # Send the client a list of all available files and their sizes
    # This is just a "browse" — no download happens
    files = list_files()

    if not files:
        send_msg(sock, "NOFILES")
        return

    # Build "filename:size" entries for each file
    parts = []
    for f in files:
        size = os.path.getsize(os.path.join(UPLOAD_DIR, f))
        parts.append(f"{f}:{size}")

    send_msg(sock, "FILELIST|" + "|".join(parts))


def h_chat(sock, addr):
    # Handle a group chat session for this client
    # The server relays messages between all connected clients
    # Protocol:
    #   Server → "CHAT_START"
    #   Client → message text OR "CHAT_QUIT"
    #   Server → "[HH:MM:SS] name: message" (echoed back + broadcast to others)
    #   Server → "CHAT_END" (when done)

    # Get this client's display name (they may have set one with SET_NAME)
    with _clients_lock:
        display_name = _clients.get(addr, {}).get("name", addr)

    send_msg(sock, "CHAT_START")

    while True:
        try:
            msg = recv_msg(sock)
        except Exception:
            break  # connection dropped — exit chat

        if msg == "CHAT_QUIT":
            break

        # Format the message with a timestamp
        timestamp = datetime.now().strftime("%H:%M:%S")
        formatted = f"[{timestamp}] {display_name}: {msg}"

        log.info(f"[{addr}] chat: {msg}")

        # Echo back to the sender so they see their own message in the chat
        send_msg(sock, formatted)

        # Send to everyone else too (group chat)
        broadcast(addr, formatted)

    send_msg(sock, "CHAT_END")


def h_server_info(sock):
    # Send the client a health report about the server
    # Includes uptime, how many clients are connected, files stored, disk space

    # Calculate how long the server has been running
    uptime = str(timedelta(seconds=int((datetime.now() - _server_start).total_seconds())))

    with _clients_lock:
        client_count = len(_clients)

    file_count = len(list_files())
    free_bytes = shutil.disk_usage(UPLOAD_DIR).free
    free_mb    = free_bytes / 1_048_576  # convert bytes to MB

    send_msg(sock, f"INFO|{uptime}|{client_count}|{file_count}|{free_mb:.0f}")


# ─── Client Handler ───────────────────────────────────────────────────────────
def handle_client(sock, address):
    # This function runs in its own thread for each connected client
    # It sets up the socket, registers the client, then loops waiting for commands

    # ── Socket tuning ─────────────────────────────────────────────────────────
    # TCP_NODELAY: send packets immediately instead of waiting to batch them
    # Without this, small messages like "PING" can be delayed by ~200ms
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    # Increase the send and receive buffers to 1MB for faster file transfers
    # 1 << 20 is a bit-shift — same as writing 1_048_576 (1 MB)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1 << 20)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1 << 20)

    # SO_KEEPALIVE: detect if the client silently disappears (e.g. power cut)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

    # If the client sends nothing for IDLE_TIMEOUT seconds, kick them
    sock.settimeout(IDLE_TIMEOUT)

    # Build the "ip:port" string used for logging and tracking this client
    addr = f"{address[0]}:{address[1]}"

    # Register this client in the shared dictionary
    with _clients_lock:
        _clients[addr] = {
            "sock"  : sock,
            "addr"  : address,
            "since" : datetime.now(),
            "name"  : addr           # default display name is their IP:port
        }
        total = len(_clients)

    log.info(f"+ {addr} connected (clients online: {total})")

    try:
        # Send a welcome message right away — client expects this first
        send_msg(sock, f"WELCOME|Synapse v2|{HOST_IP}:{HOST_PORT}|{total}")

        # ── Command loop ──────────────────────────────────────────────────────
        while not _shutdown_event.is_set():

            try:
                cmd = recv_msg(sock)  # wait here until the client sends a command
            except socket.timeout:
                log.warning(f"[{addr}] idle too long — disconnecting")
                break

            # Route the command to the right handler
            if cmd == "QUIT":
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
                # Client wants to set a display name for chat
                # cmd looks like "SET_NAME|Victor" — split at | and take the part after it
                new_name = cmd.split("|", 1)[1][:32].strip()
                with _clients_lock:
                    if addr in _clients:
                        _clients[addr]["name"] = new_name
                send_msg(sock, f"NAME_OK|{new_name}")
                log.info(f"[{addr}] set name to '{new_name}'")

            else:
                send_msg(sock, f"ERROR|Unknown command '{cmd}'")

    except ConnectionError:
        log.info(f"[{addr}] disconnected unexpectedly")

    except Exception as e:
        log.error(f"[{addr}] error: {e}", exc_info=True)

    finally:
        # Always clean up when a client leaves — whether it was planned or not
        with _clients_lock:
            _clients.pop(addr, None)  # remove from tracking dict

        try:
            sock.close()
        except Exception:
            pass

        log.info(f"- {addr} disconnected (clients online: {len(_clients)})")


# ─── Entry Point ──────────────────────────────────────────────────────────────
def main():
    # Create the main server socket
    # AF_INET = IPv4, SOCK_STREAM = TCP
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # SO_REUSEADDR lets us restart the server immediately without waiting
    # for the OS to release the port (which can take up to 2 minutes otherwise)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # Bind to our IP and port — this claims the port
    srv.bind((HOST_IP, HOST_PORT))

    # Start listening — allow up to 50 pending connections in the queue
    srv.listen(50)

    log.info("=" * 50)
    log.info("  Synapse Server v2.0")
    log.info(f"  Listening: {HOST_IP}:{HOST_PORT}")
    log.info(f"  Files dir: {os.path.abspath(UPLOAD_DIR)}")
    log.info("=" * 50)

    try:
        #Keeps the server running
        while not _shutdown_event.is_set():
            # Set a 1 second timeout so Ctrl+C can interrupt the accept() call
            srv.settimeout(1.0)

            try:
                client_sock, client_addr = srv.accept()  # wait for a connection
            except socket.timeout:
                continue  # no connection this second, loop and check shutdown flag

            # Spawn a new thread for this client so other clients aren't blocked
            # daemon=True means this thread dies automatically when the main program exits
            t = threading.Thread(target=handle_client, args=(client_sock, client_addr), daemon=True)
            t.start()

    except KeyboardInterrupt:
        log.info("Ctrl+C pressed — shutting down...")
        _shutdown_event.set()

    finally:
        # Close all active client connections
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
