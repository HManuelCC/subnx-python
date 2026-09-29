# subnx/client.py
import socket
import ssl
import json
import struct
import threading
import time
import random
import queue
import signal
from typing import Any, Callable, Optional, Dict, TYPE_CHECKING

if TYPE_CHECKING:
    from .models import Event, State

class Client:
    def __init__(self, host: str, port: str, client_name: str, api_key: str, use_tls: bool = False):
        self.host = host
        self.port = int(port)
        self.name = client_name
        self.api_key = api_key
        self.use_tls = use_tls

        self._mu = threading.RLock()       
        self._write_mu = threading.Lock()  
        self.conn: Optional[socket.socket] = None

        self._closed = False
        self._close_lock = threading.Lock() 
        
        self.is_ready = threading.Event()   
        self.done = threading.Event()       
        
        self.attempt = 1
        self.min_backoff = 1.0  
        self.max_backoff = 30.0 

        # 👈 NUEVO: Mapa seguro para procesos concurrentes (Equivalente a chan State)
        self.pending_requests: Dict[str, queue.Queue] = {}
        self.pending_requests_mu = threading.Lock()

        threading.Thread(target=self._run, daemon=True).start()

    def send(self, event: 'Event', timeout: float = 65.0, callback: Callable[['State'], None] = None) -> None:
        with self._close_lock:
            if self._closed:
                raise Exception("client cerrado")

        # 🌟 MAGIA DE CONCURRENCIA
        ready = self.is_ready.wait(timeout=5.0)

        with self._mu:
            conn_active = self.conn is not None

        if not ready or not conn_active:
            raise Exception("conexión no está lista (handshake pendiente o servidor caído)")

        return event.send_data(self, timeout, callback)

    def set_ready(self):
        self.is_ready.set()

    def close(self) -> None:
        with self._close_lock:
            if self._closed:
                return
            self._closed = True
            
        self.done.set()

        with self._mu:
            if self.conn:
                try:
                    self.conn.close()
                except:
                    pass
                self.conn = None

    def get_conn(self) -> Optional[socket.socket]:
        with self._mu:
            return self.conn

    def set_conn(self, conn: socket.socket):
        with self._mu:
            self.conn = conn

    def clear_conn(self):
        with self._mu:
            if self.conn:
                try:
                    self.conn.close()
                except:
                    pass
            self.conn = None
            self.is_ready.clear() 

    def _backoff(self) -> float:
        base = self.min_backoff * (2 ** (self.attempt - 1))
        if base > self.max_backoff:
            base = self.max_backoff
            
        jitter = random.uniform(0, base / 5)
        if random.choice([True, False]):
            return base - jitter
        return base + jitter

    def _run(self):
        from .protocol import read_loop

        while not self._closed:
            start_time = time.time()
            
            try:
                raw_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                if self.use_tls:
                    ctx = ssl.create_default_context()
                    ctx.check_hostname = False
                    ctx.verify_mode = ssl.CERT_NONE
                    self.conn = ctx.wrap_socket(raw_sock, server_hostname=self.host)
                else:
                    self.conn = raw_sock

                self.conn.connect((self.host, self.port))
                
            except Exception as e:
                delay = self._backoff()
                print(f"[client {self.name}] error al conectar: {e} (reintento en {delay:.2f}s)")
                
                if self.done.wait(timeout=delay):
                    return
                self.attempt += 1
                continue

            self.attempt = 1
            handshake_latency = int((time.time() - start_time) * 1000)
            print(f"[client {self.name}] conectado (latencia: {handshake_latency} ms)")

            server_status = queue.Queue(maxsize=1)
            
            threading.Thread(target=read_loop, args=(self, server_status, float(handshake_latency)), daemon=True).start()

            while not self.done.is_set():
                try:
                    status = server_status.get(timeout=0.5)
                    if not status:
                        print(f"[client {self.name}] desconectado por el servidor, reconectando...")
                        self.clear_conn()
                        break 
                except queue.Empty:
                    continue 

            if self.done.is_set():
                self.clear_conn()
                return

    def write_packet(self, msg_type: int, event_name: str, server_uid: str, obj_data: Any) -> None:
        try:
            data_bytes = json.dumps(obj_data).encode('utf-8')
        except Exception as e:
            raise Exception(f"error al serializar paquete: {e}")

        event_name_bytes = event_name.encode('utf-8')
        name_len = len(event_name_bytes)
        uid_bytes = server_uid.encode('utf-8').ljust(36, b'\x00')[:36]
        payload_size = len(data_bytes)

        try:
            header = struct.pack('>BB36sI', msg_type, name_len, uid_bytes, payload_size)
        except Exception as e:
            raise Exception(f"error al empaquetar el header: {e}")

        packet = header + event_name_bytes + data_bytes

        with self._write_mu:
            conn = self.get_conn()
            if not conn:
                raise Exception("conexión no disponible")
            try:
                conn.sendall(packet)
            except Exception as e:
                raise Exception(f"error al escribir en socket: {e}")

    def listen(self):
        stop_event = threading.Event()

        def handle_signal(signum, frame):
            stop_event.set()

        try:
            signal.signal(signal.SIGINT, handle_signal)
            signal.signal(signal.SIGTERM, handle_signal)
        except ValueError:
            pass

        try:
            while not stop_event.is_set():
                stop_event.wait(0.5)
        except KeyboardInterrupt:
            pass
            
        print(f"\n[client {self.name}] Apagando el cliente de forma segura...")
        self.close()