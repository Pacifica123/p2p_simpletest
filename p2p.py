# p2p.py
import socket
import threading
import time

class PeerNode:
    def __init__(self, host, port, peers=None):
        # host используется и для прослушивания, и для "саморекламы"
        # для локальных тестов укажи 127.0.0.1, а не 0.0.0.0
        self.host = host
        self.port = port
        self.node_id = f"{self.host}:{self.port}"

        self.lock = threading.Lock()
        self.known_peers = set()
        if peers:
            for h, p in peers:
                self.known_peers.add((h, p))

        # conn -> remote_id (строка "host:port") или None пока не узнали
        self.connections = {}
        self.seen_msgs = set()
        self.msg_counter = 0
        self.running = True

        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_sock.bind((self.host, self.port))
        self.server_sock.listen()
        print(f"[Node] Listening on {self.host}:{self.port}")

        threading.Thread(target=self.accept_loop, daemon=True).start()

        # начальные исходящие подключения
        for peer in list(self.known_peers):
            if peer != (self.host, self.port):
                threading.Thread(target=self.connect_to_peer, args=(peer,), daemon=True).start()

    # ---------------- net loops ----------------

    def accept_loop(self):
        while self.running:
            try:
                conn, addr = self.server_sock.accept()
            except OSError:
                break
            with self.lock:
                self.connections[conn] = None
            print(f"[Node] Accepted connection from {addr}")

            # сразу представимся и поделимся списком пиров
            self._send_line(conn, f"HELLO {self.node_id}")
            self._share_peers([conn])

            threading.Thread(target=self.handle_peer, args=(conn,), daemon=True).start()

    def connect_to_peer(self, peer):
        h, p = peer
        if (h, p) == (self.host, self.port):
            return
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(5)
            s.connect((h, p))
            s.settimeout(None)
            with self.lock:
                # не дублируем существующее подключение к тому же remote_id, но оно неизвестно до HELLO
                self.connections[s] = None
            print(f"[Node] Connected to peer {(h, p)}")

            self._send_line(s, f"HELLO {self.node_id}")
            self._share_peers([s])

            threading.Thread(target=self.handle_peer, args=(s,), daemon=True).start()
        except Exception as e:
            print(f"[Node] Could not connect to peer {(h, p)}: {e}")

    def handle_peer(self, conn):
        buf = b""
        while self.running:
            try:
                data = conn.recv(4096)
                if not data:
                    break
                buf += data
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    self._process_line(conn, line.decode(errors="ignore").strip())
            except Exception:
                break
        self._drop_conn(conn, reason="closed")

    # ---------------- protocol ----------------

    def _process_line(self, conn, line):
        if not line:
            return
        parts = line.split(" ", 2)
        cmd = parts[0].upper()

        if cmd == "HELLO" and len(parts) >= 2:
            remote_id = parts[1]
            with self.lock:
                self.connections[conn] = remote_id
                try:
                    h, sp = remote_id.split(":")
                    self.known_peers.add((h, int(sp)))
                except:
                    pass
            # обновим всем список известных пиров
            self._share_peers()
            return

        if cmd == "PEERS" and len(parts) >= 2:
            addrs = parts[1].split(",")
            to_connect = []
            for a in addrs:
                if not a:
                    continue
                try:
                    h, sp = a.split(":")
                    p = int(sp)
                except:
                    continue
                if (h, p) == (self.host, self.port):
                    continue
                with self.lock:
                    self.known_peers.add((h, p))
                    already = any((rid == f"{h}:{p}") for rid in self.connections.values() if rid)
                if not already:
                    to_connect.append((h, p))
            for peer in to_connect:
                threading.Thread(target=self.connect_to_peer, args=(peer,), daemon=True).start()
            return

        if cmd == "MSG" and len(parts) >= 3:
            msg_id = parts[1]
            rest = parts[2]
            if " " in rest:
                from_id, text = rest.split(" ", 1)
            else:
                from_id, text = "unknown", rest

            with self.lock:
                if msg_id in self.seen_msgs:
                    return
                self.seen_msgs.add(msg_id)

            print(f"[{from_id}] {text}")
            self._broadcast(line, except_conn=conn)
            return

        # fallback на случай сырого текста
        print(f"[Peer] {line}")
        self._broadcast(line, except_conn=conn)

    # ---------------- helpers ----------------

    def _send_line(self, conn, line):
        try:
            conn.sendall((line + "\n").encode())
        except Exception as e:
            self._drop_conn(conn, reason=f"send failed: {e}")

    def _share_peers(self, targets=None):
        with self.lock:
            peers_list = list(self.known_peers | {(self.host, self.port)})
            conns = list(self.connections.keys()) if targets is None else targets[:]
        if not peers_list:
            return
        payload = ",".join([f"{h}:{p}" for h, p in peers_list])
        for c in conns:
            self._send_line(c, f"PEERS {payload}")

    def _broadcast(self, line, except_conn=None):
        with self.lock:
            conns = list(self.connections.keys())
        for c in conns:
            if c is except_conn:
                continue
            self._send_line(c, line)

    def _drop_conn(self, conn, reason=""):
        with self.lock:
            if conn in self.connections:
                self.connections.pop(conn, None)
        try:
            conn.close()
        except:
            pass
        if reason:
            # можно раскомментировать для отладки
            # print(f"[Node] Drop connection: {reason}")
            pass

    # ---------------- user API ----------------

    def send_user_message(self, text):
        self.msg_counter += 1
        msg_id = f"{self.node_id}-{self.msg_counter}-{int(time.time()*1000)}"
        line = f"MSG {msg_id} {self.node_id} {text}"
        with self.lock:
            self.seen_msgs.add(msg_id)
        print(f"[me] {text}")
        self._broadcast(line)

    def stop(self):
        self.running = False
        try:
            self.server_sock.close()
        except:
            pass
        with self.lock:
            conns = list(self.connections.keys())
        for c in conns:
            try:
                c.shutdown(socket.SHUT_RDWR)
            except:
                pass
            try:
                c.close()
            except:
                pass


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print(f"Usage: python {sys.argv[0]} port [peer_host:peer_port ...]")
        sys.exit(1)

    # ВАЖНО: для локальных тестов не ставь 0.0.0.0, используй 127.0.0.1
    host = "127.0.0.1"
    port = int(sys.argv[1])
    peers = []
    for peer_arg in sys.argv[2:]:
        h, sp = peer_arg.split(":")
        peers.append((h, int(sp)))

    node = PeerNode(host, port, peers)

    try:
        while True:
            text = input(">> ")
            if text:
                node.send_user_message(text)
    except KeyboardInterrupt:
        print("\n[Node] Exiting.")
        node.stop()
