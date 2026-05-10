# Synapse v2.0

A fast, terminal-based file transfer and chat application built entirely with Python's standard library. No pip installs. No frameworks. Just raw sockets.

---

## What is this?

Synapse lets two or more computers on the same network (or over the internet) transfer files and chat with each other in real time. One machine runs `server.py` and acts as the hub. Any number of clients run `client.py` and connect to it.

Think of it like a private, self-hosted Xender/ShareIt — but in your terminal, with progress bars, group chat, batch uploads, and SHA-256 file verification built in.

---

## Requirements

- Python 3.10 or newer (uses the `int | None` type hint syntax)
- Both machines must be on the same network, OR the server machine must have a public IP / port forwarded
- No third-party libraries needed — everything is Python standard library

---

## Project Structure

```
your_project/
│
├── server.py          ← Run this on the host machine
├── client.py          ← Run this on any machine that wants to connect
│
├── server_files/      ← Created automatically. All uploaded files land here.
├── client_downloads/  ← Created automatically. All downloaded files land here.
└── logs/
    └── server.log     ← Server activity log (created automatically)
```
---

## Quick Start

### Step 1 — Start the server

Open a terminal on the host machine and run:

```bash
python server.py
```

You will see something like:

```
========================================================
   Synapse  Server  v2.0
   Listening  :  192.168.1.5:1234
   Files dir  :  /home/victor/server_files
   Chunk size :  128 KB
   Idle limit :  300s
========================================================
```
<img width="1600" height="860" alt="synapse" src="https://github.com/user-attachments/assets/9c13dab2-2a56-43aa-9aa3-b599684e8cc0" />
Note down the IP address shown (`192.168.1.5` in this example). Clients need it.

### Step 2 — Connect a client

On any other machine (or the same machine for testing), run:

```bash
python client.py
```

You will be asked for the server IP and port:

```
  Server IP    [127.0.0.1]:   192.168.1.5
  Server Port  [1234]:
```

Press Enter on the port if you left it as default (1234). If connection succeeds you will see the main menu.

---

## Client Menu

```
  ╔══════════════════════════════════════════════╗
  ║   Synapse  ·  Client  v2.0            ║
  ╠══════════════════════════════════════════════╣
  ║  1  Upload a file                           ║
  ║  2  Upload multiple files / folder          ║
  ║  3  Browse server files                     ║
  ║  4  Download a file                         ║
  ║  5  Group chat                              ║
  ║  6  Ping server                             ║
  ║  7  Server health info                      ║
  ║  8  Set my chat name                        ║
  ║  9  My downloaded files                     ║
  ║  q  Quit                                    ║
  ╚══════════════════════════════════════════════╝
```
<img width="1600" height="860" alt="synapse2" src="https://github.com/user-attachments/assets/78c84cb6-ac5c-4e48-b2b5-440ed2a718c2" />

---

## Feature Guide

### 1 — Upload a file

Uploads one file to the server. You will be prompted for the file path.

```
  File path: /home/victor/video.mp4
```

What happens behind the scenes:
1. The client computes a SHA-256 fingerprint of the file before sending
2. The file is streamed in 128 KB chunks with a live progress bar
3. The server recomputes the SHA-256 after receiving every byte
4. If both fingerprints match, the file is saved. If they don't, the file is deleted and you get an error. This guarantees you will never silently receive a corrupted file.

---

### 2 — Upload multiple files / folder

Upload an entire folder or a comma-separated list of files in one session.

Examples of what you can type when prompted:

```
  /home/victor/photos                          ← uploads the whole folder
  /home/victor/a.pdf, /home/victor/b.zip      ← uploads two specific files
```

Each file shows its own progress bar. At the end you get a summary:

```
  ✓  Batch complete: 5 succeeded, 0 failed
```

---

### 3 — Browse server files

Lists every file stored on the server with its size, without downloading anything. Useful for deciding what you want before committing to a download.

```
  #    Filename                                  Size
  ───  ────────────────────────────────────────  ──────────
    1  project_backup.zip                        142.3 MB
    2  notes.pdf                                   1.1 MB
    3  photo.jpg                                 800.0 KB
```



---

### 4 — Download a file

Shows the list of available files and lets you pick one by number. A progress bar tracks the download in real time. SHA-256 is verified on arrival — if the file is corrupted in transit, it is automatically deleted and you are told to try again.

Downloaded files land in `client_downloads/`. If a file with the same name already exists, a timestamp is added to the new one so nothing is overwritten.

---
<img width="1600" height="860" alt="synapse3" src="https://github.com/user-attachments/assets/3d20d6ad-3844-4f16-8b9b-a3fb815393bd" />
### 5 — Group chat

Sends a message to every connected client at once. All clients in chat mode will see each other's messages with a timestamp and the sender's display name (set with option 8).

A background thread listens for incoming messages while you type, so you never miss a message even if you are mid-sentence.

Type `quit` to leave chat and return to the menu.

---
<img width="1600" height="860" alt="synapse4" src="https://github.com/user-attachments/assets/d62f8624-c15b-416f-a910-3b60ce9693c2" />

### 6 — Ping server

Sends 5 pings and measures the round-trip time (RTT) for each one, then shows average, minimum, and maximum.

```
  [1] pong   RTT =  1.24 ms
  [2] pong   RTT =  1.18 ms
  [3] pong   RTT =  1.31 ms
  [4] pong   RTT =  1.22 ms
  [5] pong   RTT =  1.19 ms

  avg 1.23 ms   min 1.18 ms   max 1.31 ms
```

RTT is the time from when you send a PING to when you receive PONG back. A high RTT means a slow or congested network connection.

---

### 7 — Server health info

Asks the server for a live snapshot of its current state:

```
  Uptime       :  0:42:17
  Clients      :  3
  Stored files :  12
  Disk free    :  48231 MB
```

---

### 8 — Set my chat name

By default you appear in chat as your IP address and port (e.g. `192.168.1.7:54321`). Use this option to set a readable name like `Victor` instead. The name is shown to all other clients in the chat.

---

### 9 — My downloaded files

Lists all files you have previously downloaded, stored in your local `client_downloads/` folder.

---

## Configuration

Open the relevant file in a text editor and change these values near the top.

### server.py

| Variable | Default | What it does |
|---|---|---|
| `HOST_PORT` | `1234` | Port the server listens on |
| `BYTESIZE` | `131072` | Chunk size (128 KB). Increase on fast LANs. |
| `UPLOAD_DIR` | `"server_files"` | Where uploaded files are stored |
| `IDLE_TIMEOUT` | `300` | Seconds of inactivity before a client is dropped |

### client.py

| Variable | Default | What it does |
|---|---|---|
| `DEFAULT_IP` | `"127.0.0.1"` | Pre-filled server IP (change to avoid typing it each time) |
| `DEFAULT_PORT` | `1234` | Pre-filled server port |
| `BYTESIZE` | `131072` | Must match server's chunk size |
| `DOWNLOAD_DIR` | `"client_downloads"` | Where downloaded files are saved |
| `MAX_RETRIES` | `4` | How many reconnect attempts before giving up |
| `RETRY_BASE` | `1.5` | Controls reconnect wait time (exponential back-off) |

If you change `HOST_PORT` on the server you must change `DEFAULT_PORT` on the client to match.

---

## How the Protocol Works

Every single message sent between server and client uses **length-prefixed framing**. This is the technique that prevents messages from getting merged or split during transmission.

Here is how it works:

```
┌───────────────────┬──────────────────────────────────┐
│  4 bytes (length) │  N bytes (the actual message)    │
└───────────────────┴──────────────────────────────────┘
```

Before sending any text, the app converts it to bytes, measures the length, and sends that length as a 4-byte number first. The receiver reads the 4-byte number first, then knows exactly how many bytes to wait for. This means messages can never bleed into each other no matter how fast or slow the network is.

### Command table

| Client sends | Server responds | Meaning |
|---|---|---|
| `PING` | `PONG` | Latency check |
| `SEND_FILE` | `READY` | Begin single file upload |
| `SEND_MULTI` | `READY` | Begin batch upload |
| `GET_FILE` | `FILES\|...` or `NOFILES` | Begin download |
| `LIST_FILES` | `FILELIST\|...` or `NOFILES` | Browse server files |
| `CHAT` | `CHAT_START` | Enter group chat |
| `SERVER_INFO` | `INFO\|...` | Request server health |
| `SET_NAME\|name` | `NAME_OK\|name` | Set chat display name |
| `QUIT` | `BYE` | Disconnect cleanly |

---

## Why it's Fast (Technical Explanation)

Three socket options are set on every connection that make a measurable difference.

### TCP_NODELAY

TCP normally holds small outgoing packets in a buffer and waits for more data to arrive before sending — this is called Nagle's Algorithm. It is designed to save bandwidth on slow networks. The problem is that in a command-response app (send "PING", wait for "PONG"), the "PING" sits in that buffer for up to 200ms before being sent. That delay is what makes interactive socket apps feel laggy.

`TCP_NODELAY = 1` disables Nagle's Algorithm. Every write is sent immediately, even if it is just 4 bytes. This is the single biggest fix for perceived lag.

### SO_SNDBUF and SO_RCVBUF

The OS maintains internal buffers for data waiting to be sent and data waiting to be read. The default is typically 8–64 KB. Both files set this to 1 MB. This means more data can be in-flight at once without the kernel pausing the transfer to drain the queue. The result is noticeably higher throughput on large file transfers.

### SO_KEEPALIVE

If a client machine crashes or loses power, the server would normally never know and would hold that connection open forever. `SO_KEEPALIVE = 1` tells the OS to periodically send a tiny heartbeat packet. If no response comes back, the OS marks the connection as dead and the server thread can clean up.

---

## Running on a Server (Linux / Nginx / Gunicorn context)

This app does not use HTTP so Nginx and Gunicorn are not involved. It is a raw TCP server. To run it permanently in the background on a Linux server:

```bash
# Using screen (simple)
screen -S xender
python server.py
# Press Ctrl+A then D to detach. Run `screen -r xender` to reattach.

# Using nohup (even simpler)
nohup python server.py &> logs/server.log &
```

Make sure port 1234 is open in your firewall:

```bash
sudo ufw allow 1234/tcp
```

---

## Troubleshooting

**"Connection refused"**
The server is not running, or you typed the wrong IP/port. Double-check the IP shown when you start `server.py`.

**"Address already in use" when starting the server**
Another process is using port 1234, or the previous server run did not shut down cleanly. Either wait 30 seconds or change `HOST_PORT` in `server.py`.

**Progress bar shows 0.00 MB/s for a moment**
Normal. It takes a fraction of a second for the first chunk to move. The speed display updates every 150ms.

**Downloaded file was deleted after "Hash mismatch"**
The file was corrupted in transit. This is rare on a LAN but can happen on unstable connections. Simply download it again.

**Chat messages appear jumbled with my typing**
This is a limitation of raw terminal input — there is no proper TUI (terminal UI) library in use to separate input from output lines. It does not affect delivery of messages.

---

## Stopping the Server

Press `Ctrl+C` in the server terminal. The server will close all active client connections cleanly before exiting.
<img width="1600" height="860" alt="synapse5" src="https://github.com/user-attachments/assets/4ca97805-8b9a-48e6-a4ec-a9d45f4ad521" />
