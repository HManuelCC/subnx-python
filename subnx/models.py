# subnx/models.py
import json
import uuid
import threading
import queue
import time
from typing import Any, Callable, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .client import Client

class ClientHardwareResourcesStatistics:
    def __init__(self):
        self.cpu_usage: float = 0.0
        self.memory_usage: float = 0.0
        self.disk_usage: float = 0.0
        self.disk_busy: float = 0.0

    def to_dict(self) -> dict:
        return {
            "cpu_usage": self.cpu_usage,
            "memory_usage": self.memory_usage,
            "disk_usage": self.disk_usage,
            "disk_busy": self.disk_busy
        }

class EventsSubscribed:
    def __init__(self, events: List[str]):
        self.events = events

    def to_dict(self) -> dict:
        return {"events": self.events}

class ClientInformation:
    def __init__(self, client_name: str, api_key: str, latency: float, 
                 resources: ClientHardwareResourcesStatistics, events: EventsSubscribed):
        self.client_name = client_name
        self.api_key = api_key
        self.latency = latency
        self.resources = resources
        self.events = events

    def to_dict(self) -> dict:
        return {
            "client_name": self.client_name,
            "api_key": self.api_key,
            "latency": self.latency,
            "resources": self.resources.to_dict(),
            "events": self.events.to_dict()
        }

class State:
    def __init__(self, status: bool, message: str, error: str = "", data: Any = None, uid: str = ""):
        self.status = status
        self.message = message
        self.error = error
        self.uid = uid
        self.server_uid = ""
        self.data = data
        self._raw_data: str = "" # 🌟 MAGIA: Bytes crudos de Go

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "message": self.message,
            "error": self.error,
            "uid": self.uid,
            "data": self.data
        }

    # 🌟 METODO BIND ENCAPSULADO
    def bind(self, target: Any) -> None:
        if not self._raw_data:
            return
        parsed = json.loads(self._raw_data)
        if isinstance(parsed, dict):
            if isinstance(target, dict):
                target.update(parsed)
            elif hasattr(target, '__dict__'):
                for k, v in parsed.items():
                    setattr(target, k, v)
        elif isinstance(parsed, list) and isinstance(target, list):
            target.clear()
            target.extend(parsed)

    def to_string(self) -> str:
        return f"State{{Status: {self.status}, Message: '{self.message}', Error: '{self.error}', Data: {self.data}, UID: '{self.uid}'}}"

class Event:
    def __init__(self, event: str, data: Any, uid: str = ""):
        self.event = event
        self.data = data
        self.uid = uid
        self.server_uid = ""
        self._raw_data: str = "" # 🌟 MAGIA: Bytes crudos de Go
        
        # Emulación de los canales y sync.Once
        self.replied = threading.Event()
        self.extended = threading.Event()
        self._reply_once = threading.Lock()
        self._has_replied = False

    def to_dict(self) -> dict:
        return {
            "event": self.event,
            "data": self.data,
            "uid": self.uid
        }

    # 🌟 METODO BIND ENCAPSULADO
    def bind(self, target: Any) -> None:
        if not self._raw_data:
            return
        parsed = json.loads(self._raw_data)
        if isinstance(parsed, dict):
            if isinstance(target, dict):
                target.update(parsed)
            elif hasattr(target, '__dict__'):
                for k, v in parsed.items():
                    setattr(target, k, v)
        elif isinstance(parsed, list) and isinstance(target, list):
            target.clear()
            target.extend(parsed)

    # 🌟 ENCAPSULACIÓN: SendData le pertenece a Event
    def send_data(self, client: 'Client', timeout_sec: Optional[float] = 65.0, callback: Optional[Callable[[State], None]] = None) -> None:
        self.uid = str(uuid.uuid4())

        # 1. Enviamos el Tipo 1 y le pasamos e.event
        client.write_packet(1, self.event, "", self.to_dict())

        # req := &pendingRequest{ Data: make(chan State, 1) }
        req_queue = queue.Queue(maxsize=1) 
        
        with client.pending_requests_mu:
            client.pending_requests[self.uid] = req_queue

        if timeout_sec is None:
            timeout_sec = 65.0

        if callback is not None:
            def wait_sync():
                try:
                    state = req_queue.get(timeout=timeout_sec)
                    if state.status:
                        callback(state)
                    else:
                        print(f"error del servidor: {state.error}")
                except queue.Empty:
                    with client.pending_requests_mu:
                        client.pending_requests.pop(self.uid, None)
                    print(f"timeout esperando respuesta para UID {self.uid}")
            threading.Thread(target=wait_sync, daemon=True).start()
        else:
            def wait_async():
                try:
                    state = req_queue.get(timeout=timeout_sec)
                    if not state.status:
                        print(f"Respuesta asíncrona fallida: {state.error}")
                except queue.Empty:
                    with client.pending_requests_mu:
                        client.pending_requests.pop(self.uid, None)
            threading.Thread(target=wait_async, daemon=True).start()

    # 🌟 ENCAPSULACIÓN: Reply le pertenece a Event
    def reply(self, client: 'Client', state: State):
        # e.replyOnce.Do(...)
        with self._reply_once:
            if not self._has_replied:
                self.replied.set() # close(e.replied)
                self._has_replied = True

        state.uid = self.uid
        client.write_packet(2, "", self.server_uid, state.to_dict())


class MessageState:
    def __init__(self, status: bool, server_uid: str, message: str, error: str, process_status: int):
        self.status = status
        self.server_uid = server_uid
        self.message = message
        self.error = error
        self.process_status = process_status

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "server_uid": self.server_uid,
            "state": self.message,
            "error": self.error,
            "process_status": self.process_status
        }
        
    # 🌟 ENCAPSULACIÓN: send le pertenece a MessageState
    def send(self, client: 'Client'):
        client.write_packet(3, "", self.server_uid, self.to_dict())

    def to_string(self) -> str:
        return f"MessageState{{Status: {self.status}, ServerUID: '{self.server_uid}', Message: '{self.message}', Error: '{self.error}'}}"

# Tipo equivalente a: type ResponseCallback func(response State)
ResponseCallback = Callable[[State], None]