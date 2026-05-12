import socket
import hashlib
import os
import sys
import time
import threading
from datetime import datetime


# ─── ANSI Colors ──────────────────────────────────────────────────────────────
# ANSI codes are special sequences that tell the terminal to change text color.
# Format: \033[<number>m  — \033 is the ESC character, m ends the sequence.
# Always end colored text with RESET so the color doesn't bleed into the next line.

class C:
    RESET   = "\033[0m"    # back to normal
    BOLD    = "\033[1m"
    DIM     = "\033[2m"
    RED     = "\033[91m"
    GREEN   = "\033[92m"
    YELLOW  = "\033[93m"
    BLUE    = "\033[94m"
    MAGENTA = "\033[95m"
    CYAN    = "\033[96m"
    WHITE   = "\033[97m"


# ─── Print Helpers ────────────────────────────────────────────────────────────
def ok(msg):
    print(f"{C.GREEN}  ✓  {msg}{C.RESET}")

def err(msg):
    print(f"{C.RED}  ✗  {msg}{C.RESET}")

def info(msg):
    print(f"{C.CYAN}  ℹ  {msg}{C.RESET}")

def warn(msg):
    print(f"{C.YELLOW}  ⚠  {msg}{C.RESET}")

def banner(title):
    # Prints a section header with a border around it
    line = f"{C.BOLD}{C.BLUE}{'═' * 58}{C.RESET}"
    print(f"\n{line}")
    print(f"{C.BOLD}{C.WHITE}   {title}{C.RESET}")
    print(line)


# ─── Config ───────────────────────────────────────────────────────────────────
DEFAULT_IP   = "127.0.0.1"   # server IP — change this if server is on another machine
DEFAULT_PORT = 1234           # must match HOST_PORT in server.py
ENCODER      = "utf-8"
BYTESIZE     = 131072         # 128 KB — same as the server
DOWNLOAD_DIR = "client_downloads"
MAX_RETRIES  = 4              # how many times to retry connecting
RETRY_BASE   = 1.5            # base for exponential backoff (1.5^attempt seconds)

os.makedirs(DOWNLOAD_DIR, exist_ok=True)  # create download folder if missing


# ─── Protocol: Length-Prefixed Messages ───────────────────────────────────────
# Same protocol as the server — must match exactly.
# Every message is sent as: [4-byte length][message bytes]
# The receiver reads the 4 bytes first to know how many bytes to read next.

def read_exactly(sock, n):
    # Loop until we've received exactly n bytes from the socket
    buf = bytearray()
    while len(buf) < n:
        remaining = n - len(buf)
        chunk = sock.recv(min(remaining, BYTESIZE))
        if not chunk:
            raise ConnectionError("Server closed the connection")
        buf += chunk
    return bytes(buf)


def recv_msg(sock):
    # Read the 4-byte length header, then read that many bytes
    length = int.from_bytes(read_exactly(sock, 4), "big")
    return read_exactly(sock, length).decode(ENCODER)


def send_msg(sock, text):
    # Encode text, prepend its length as 4 bytes, send everything
    data = text.encode(ENCODER)
    sock.sendall(len(data).to_bytes(4, "big") + data)


# ─── Utility Functions ────────────────────────────────────────────────────────
def sha256_file(path):
    # Compute the SHA-256 fingerprint of a file, reading chunk by chunk
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(BYTESIZE)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()  # 64-character hex string


def fmt_size(n):
    # Convert bytes to human readable string: 2097152 → "2.0 MB"
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def progress_bar(done, total, speed, width=36):
    # Build a text progress bar like: [██████░░░░░░]  50.0%  1.0 MB/2.0 MB  45.00 MB/s
    pct  = done / total if total else 1.0        # fraction completed (0.0 to 1.0)
    fill = int(width * pct)                       # how many filled blocks
    bar  = "█" * fill + "░" * (width - fill)     # build the bar string

    return (
        f"{C.CYAN}[{bar}]{C.RESET} "
        f"{C.BOLD}{pct*100:5.1f}%{C.RESET}  "
        f"{fmt_size(done):>10}/{fmt_size(total):<10}  "
        f"{C.YELLOW}{speed:6.2f} MB/s{C.RESET}"
    )


def print_progress(done, total, t0):
    # Print the progress bar on the same line (overwrites itself using \r)
    elapsed = max(time.perf_counter() - t0, 1e-9)  # seconds since transfer started
    speed   = done / elapsed / 1_048_576             # MB/s
    print(f"\r  {progress_bar(done, total, speed)}", end="", flush=True)


def unique_local_path(filename):
    # Return a safe download path — adds timestamp if file already exists
    safe_name = os.path.basename(filename)  # strip any folder parts from filename
    dest = os.path.join(DOWNLOAD_DIR, safe_name)

    if not os.path.exists(dest):
        return dest

    # File exists — append timestamp to avoid overwriting
    name, ext = os.path.splitext(safe_name)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(DOWNLOAD_DIR, f"{name}_{timestamp}{ext}")


# ─── Connection ───────────────────────────────────────────────────────────────
# We store the current server IP and port here so reconnect() can reach them
_server_ip   = DEFAULT_IP
_server_port = DEFAULT_PORT


def make_socket():
    # Create and tune a TCP socket for low latency + high throughput
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # TCP_NODELAY: disable Nagle's algorithm — send packets immediately
    # Without this, small messages sit in a buffer waiting for more data (~200ms delay)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    # Increase send/receive buffers to 1 MB for better file transfer performance
    s.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1 << 20)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1 << 20)

    # Detect if the server silently disappears (e.g. crashed, power cut)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

    return s


def connect(ip, port, silent=False):
    # Try to connect to the server, retrying with increasing delays on failure
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if not silent:
                info(f"Connecting to {ip}:{port} (attempt {attempt}/{MAX_RETRIES})...")

            s = make_socket()
            s.settimeout(8)     # give up if connect() takes more than 8 seconds
            s.connect((ip, port))
            s.settimeout(None)  # back to blocking mode — no timeout after connecting

            # The server sends a WELCOME message right after accepting the connection
            welcome = recv_msg(s)
            parts   = welcome.split("|")

            if parts[0] == "WELCOME":
                ok(f"Connected to {parts[1]} at {parts[2]}")
                ok(f"Clients online: {parts[3]}")

            return s  # return the live socket

        except ConnectionRefusedError:
            warn(f"Attempt {attempt} failed — server refused connection")
        except socket.timeout:
            warn(f"Attempt {attempt} timed out after 8s")
        except Exception as e:
            warn(f"Attempt {attempt} error: {e}")

        # Wait before retrying — delay grows with each failed attempt
        if attempt < MAX_RETRIES:
            delay = RETRY_BASE ** attempt  # 1.5s, 2.25s, 3.375s ...
            info(f"Retrying in {delay:.1f}s...")
            time.sleep(delay)

    err("Could not connect after all attempts.")
    return None


def reconnect(ip, port):
    warn("Connection lost — attempting to reconnect...")
    return connect(ip, port)


# ─── Command Functions ────────────────────────────────────────────────────────

def upload_one(sock, path, label=""):
    # Upload a single file — used by both cmd_send_file and cmd_send_multi
    # Protocol (called after server sends "READY"):
    #   Client → filename, filesize (string), sha256
    #   Client → raw file bytes
    #   Server → "OK|saved_name|speed" or "FAIL|reason"

    filename = os.path.basename(path)
    filesize = os.path.getsize(path)

    # Hash the file before sending — we'll compare with the server's hash later
    info(f"Hashing {label or filename} ({fmt_size(filesize)})...")
    file_hash = sha256_file(path)
    ok(f"SHA-256: {file_hash[:20]}...")

    # Send metadata first so the server knows what's coming
    send_msg(sock, filename)
    send_msg(sock, str(filesize))
    send_msg(sock, file_hash)

    info(f"Uploading {label or filename}...")

    sent = 0
    t0   = time.perf_counter()
    last = t0

    with open(path, "rb") as f:
        while True:
            chunk = f.read(BYTESIZE)
            if not chunk:
                break
            sock.sendall(chunk)
            sent += len(chunk)

            # Only update the progress bar ~6 times per second (every 150ms)
            # Updating it thousands of times per second wastes CPU
            now = time.perf_counter()
            if now - last >= 0.15:
                print_progress(sent, filesize, t0)
                last = now

    # Final update to show 100% when done
    print_progress(filesize, filesize, t0)
    print()  # move to next line after the progress bar

    result = recv_msg(sock)
    parts  = result.split("|")

    if parts[0] == "OK":
        speed = float(parts[2]) if len(parts) > 2 else 0.0
        ok(f"Saved as '{parts[1]}' at {speed:.2f} MB/s")
        return True
    else:
        err(f"Upload failed: {'|'.join(parts[1:])}")
        return False


def cmd_send_file(sock):
    banner("Upload File")

    path = input(f"{C.CYAN}  File path: {C.RESET}").strip().strip('"')

    if not path:
        warn("Cancelled.")
        return

    if not os.path.isfile(path):
        err(f"File not found: {path}")
        return

    send_msg(sock, "SEND_FILE")

    if recv_msg(sock) != "READY":
        err("Server is not ready.")
        return

    upload_one(sock, path)


def cmd_send_multi(sock):
    banner("Batch Upload")

    raw = input(f"{C.CYAN}  File paths (comma-separated) or folder: {C.RESET}").strip().strip('"')

    if not raw:
        warn("Cancelled.")
        return

    # Collect all file paths to upload
    paths = []

    for entry in raw.split(","):
        entry = entry.strip().strip('"')

        if os.path.isfile(entry):
            paths.append(entry)

        elif os.path.isdir(entry):
            # If it's a folder, add all files inside it
            for f in sorted(os.listdir(entry)):
                full = os.path.join(entry, f)
                if os.path.isfile(full):
                    paths.append(full)
        else:
            warn(f"Skipping (not found): {entry}")

    if not paths:
        err("No valid files to upload.")
        return

    info(f"{len(paths)} file(s) queued.")

    send_msg(sock, "SEND_MULTI")

    if recv_msg(sock) != "READY":
        err("Server is not ready.")
        return

    # Tell the server how many files are coming so it knows when to stop
    send_msg(sock, str(len(paths)))

    n_ok   = 0
    n_fail = 0

    for i, path in enumerate(paths, 1):
        print(f"\n{C.BOLD}{C.MAGENTA}  [{i}/{len(paths)}] {os.path.basename(path)}{C.RESET}")
        success = upload_one(sock, path, label=f"[{i}/{len(paths)}]")
        if success:
            n_ok += 1
        else:
            n_fail += 1

    recv_msg(sock)  # receive the MULTI_DONE summary (we're already tracking counts)
    print()
    ok(f"Batch done: {n_ok} succeeded, {n_fail} failed")


def cmd_list_server_files(sock):
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

    entries = parts[1:]  # each entry is "filename:size_in_bytes"

    print(f"\n  {'#':>3}   {'Filename':<40}  {'Size':>10}")
    print(f"  {'─'*3}   {'─'*40}  {'─'*10}")

    for i, entry in enumerate(entries, 1):
        # rsplit splits from the right — safe if filename contains colons
        name, size_str = entry.rsplit(":", 1)
        size_human     = fmt_size(int(size_str))
        print(f"  {C.YELLOW}{i:>3}{C.RESET}   {name:<40}  {C.DIM}{size_human:>10}{C.RESET}")

    print()


def cmd_get_file(sock):
    banner("Download File")

    send_msg(sock, "GET_FILE")
    resp = recv_msg(sock)

    if resp == "NOFILES":
        warn("No files on the server to download.")
        return

    parts = resp.split("|")

    if parts[0] != "FILES":
        err(f"Unexpected response: {resp}")
        return

    files = parts[1:]  # list of available filenames

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

    # META format: "META|filename|bytecount|sha256"
    mparts   = meta.split("|")
    fname    = mparts[1]
    filesize = int(mparts[2])
    expected = mparts[3]

    info(f"File    : {fname}")
    info(f"Size    : {fmt_size(filesize)}")
    info(f"SHA-256 : {expected[:20]}...")

    # Prepare a safe local path (won't overwrite existing files)
    dest = unique_local_path(fname)

    # Tell the server to start sending
    send_msg(sock, "ACK")

    info("Downloading...")

    received = 0
    hasher   = hashlib.sha256()
    t0       = time.perf_counter()
    last     = t0

    try:
        with open(dest, "wb") as f:
            while received < filesize:
                # Only ask for as many bytes as we still need
                to_read = min(BYTESIZE, filesize - received)
                chunk   = sock.recv(to_read)

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
            os.remove(dest)  # delete partial/corrupt file
        err(f"Download failed: {e}")
        return

    # Verify the file arrived intact
    if hasher.hexdigest() == expected and received == filesize:
        elapsed = max(time.perf_counter() - t0, 1e-9)
        speed   = received / elapsed / 1_048_576
        ok(f"Downloaded '{os.path.basename(dest)}' at {speed:.2f} MB/s")
        ok(f"Saved to: {os.path.abspath(dest)}")
    else:
        os.remove(dest)
        err("Hash mismatch — corrupted file deleted. Please try again.")


def cmd_chat(sock):
    banner("Group Chat")

    send_msg(sock, "CHAT")

    resp = recv_msg(sock)
    if resp != "CHAT_START":
        err(f"Unexpected response: {resp}")
        return

    info("Chat started. Type messages and press Enter. Type 'quit' to leave.\n")

    # We use a threading.Event as a shared on/off flag between threads
    # When stop_flag is set, both threads know it's time to stop
    stop_flag = threading.Event()

    def listen_for_messages():
        # This runs in a background thread — it just waits for incoming messages
        # and prints them as they arrive. The main thread handles user input.
        while not stop_flag.is_set():
            try:
                msg = recv_msg(sock)
            except Exception:
                stop_flag.set()
                break

            if msg == "CHAT_END":
                stop_flag.set()
                break

            # \r clears the current line (erases any partially typed input)
            # Then we reprint the "You: " prompt so it looks clean
            print(f"\r{C.GREEN}  {msg}{C.RESET}\n{C.CYAN}  You: {C.RESET}", end="", flush=True)

    # Start the listener thread as a daemon — it dies when the main program exits
    listener = threading.Thread(target=listen_for_messages, daemon=True)
    listener.start()

    # Main thread: handle user input
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

    listener.join(timeout=2)  # wait for listener to finish (max 2 seconds)
    ok("Left the chat.")


def cmd_ping(sock):
    banner("Ping")

    samples = []

    for i in range(5):
        t0 = time.perf_counter()
        send_msg(sock, "PING")
        resp = recv_msg(sock)
        rtt  = (time.perf_counter() - t0) * 1000  # convert to milliseconds

        if resp == "PONG":
            samples.append(rtt)
            print(f"  [{i+1}] {C.GREEN}pong{C.RESET}   RTT = {rtt:.2f} ms")
        else:
            print(f"  [{i+1}] {C.RED}??{C.RESET}   RTT = {rtt:.2f} ms")

        time.sleep(0.1)  # small pause between pings

    if samples:
        avg = sum(samples) / len(samples)
        mn  = min(samples)
        mx  = max(samples)
        print(f"\n  {C.BOLD}avg {avg:.2f} ms   min {mn:.2f} ms   max {mx:.2f} ms{C.RESET}")


def cmd_server_info(sock):
    banner("Server Info")

    send_msg(sock, "SERVER_INFO")
    resp = recv_msg(sock)

    parts = resp.split("|")

    if parts[0] != "INFO":
        err(f"Unexpected response: {resp}")
        return

    uptime, clients, files, disk_mb = parts[1], parts[2], parts[3], parts[4]

    print(f"\n  Uptime       :  {C.CYAN}{uptime}{C.RESET}")
    print(f"  Clients      :  {C.CYAN}{clients}{C.RESET}")
    print(f"  Stored files :  {C.CYAN}{files}{C.RESET}")
    print(f"  Disk free    :  {C.CYAN}{float(disk_mb):.0f} MB{C.RESET}\n")


def cmd_set_name(sock):
    name = input(f"{C.CYAN}  Your chat name (max 32 chars): {C.RESET}").strip()

    if not name:
        warn("Cancelled.")
        return

    # Send the name to the server — only send the first 32 characters
    send_msg(sock, f"SET_NAME|{name[:32]}")

    resp  = recv_msg(sock)
    parts = resp.split("|")

    if parts[0] == "NAME_OK":
        ok(f"Name set to '{parts[1]}'")
    else:
        err(f"Unexpected response: {resp}")


def cmd_list_local():
    banner("My Downloaded Files")

    # List only actual files in the download folder
    files = []
    for f in os.listdir(DOWNLOAD_DIR):
        if os.path.isfile(os.path.join(DOWNLOAD_DIR, f)):
            files.append(f)
    files.sort()

    if not files:
        warn(f"No files in {DOWNLOAD_DIR}/")
        return

    for f in files:
        size = os.path.getsize(os.path.join(DOWNLOAD_DIR, f))
        print(f"  {C.YELLOW}•{C.RESET}  {f:<44}  {C.DIM}{fmt_size(size):>10}{C.RESET}")

    print()


# ─── Menu ─────────────────────────────────────────────────────────────────────
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


# ─── Entry Point ──────────────────────────────────────────────────────────────
def main():
    global _server_ip, _server_port  # allow this function to update the globals

    print(f"\n{C.BOLD}{C.CYAN}  ╔══════════════════════════════╗")
    print(f"  ║   Synapse  v2.0              ║")
    print(f"  ╚══════════════════════════════╝{C.RESET}\n")

    # Ask the user for the server address (press Enter to use the default)
    ip_in   = input(f"{C.CYAN}  Server IP   [{DEFAULT_IP}]:   {C.RESET}").strip()
    port_in = input(f"{C.CYAN}  Server Port [{DEFAULT_PORT}]:      {C.RESET}").strip()

    # Use what was typed, or fall back to the default if they just pressed Enter
    _server_ip   = ip_in   if ip_in              else DEFAULT_IP
    _server_port = int(port_in) if port_in.isdigit() else DEFAULT_PORT

    # Connect to the server (retries automatically if it fails)
    sock = connect(_server_ip, _server_port)

    if not sock:
        sys.exit(1)  # exit with error code 1 so the shell knows something went wrong

    # ── Main Loop ─────────────────────────────────────────────────────────────
    while True:
        print(MENU)

        try:
            choice = input(f"\n{C.CYAN}  › {C.RESET}").strip().lower()
        except (EOFError, KeyboardInterrupt):
            choice = "q"

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
                    recv_msg(sock)  # receive the server's "BYE" message
                except Exception:
                    pass
                ok("Disconnected. Goodbye!")
                break
            elif choice == "":
                pass  # user pressed Enter with nothing — just show the menu again
            else:
                warn(f"Unknown option '{choice}'")

        except ConnectionError as e:
            # Connection dropped mid-command — try to reconnect
            err(f"Connection lost: {e}")
            new_sock = reconnect(_server_ip, _server_port)
            if new_sock:
                sock = new_sock
                ok("Reconnected! You can continue.")
            else:
                err("Could not reconnect. Exiting.")
                break

        except Exception as e:
            err(f"Unexpected error: {e}")
            # Keep running — the user can try another option

    # ── Cleanup ───────────────────────────────────────────────────────────────
    try:
        sock.close()
    except Exception:
        pass


# Only run main() if this file is executed directly (not imported as a module)
if __name__ == "__main__":
    main()