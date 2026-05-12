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