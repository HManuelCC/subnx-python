# subnx/protocol.py
import struct
import json
import queue
import socket
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .client import Client

def _recv_exactly(conn: socket.socket, n: int) -> bytes:
    data = bytearray()
    while len(data) < n:
        packet = conn.recv(n - len(data))
        if not packet:
            raise Exception("EOF")
        data.extend(packet)
    return bytes(data)

def read_loop(client: 'Client', server_status: queue.Queue, latency: float):
    from .models import Event, State, MessageState
    from .handlers import handle_events

    while not client._closed:
        try:
            conn = client.get_conn()
            if not conn:
                raise Exception("EOF")

            # 1. LEER PREFIJO FIJO (42 Bytes)
            header = _recv_exactly(conn, 42)
            msg_type, name_len, uid_bytes, payload_size = struct.unpack('>BB36sI', header)
            
            message_uid = uid_bytes.decode('utf-8').rstrip('\x00')

            # 2. LEER CUERPO DINÁMICO
            body = _recv_exactly(conn, name_len + payload_size)
            
            # 3. SEPARAMOS LOS DATOS
            event_name = body[:name_len].decode('utf-8')
            data_bytes = body[name_len:]

            try:
                obj = json.loads(data_bytes.decode('utf-8'))
            except:
                continue

            if msg_type == 1:
                event_val = event_name if event_name else obj.get('event', '')
                
                # 🌟 TRASLADO MÁGICO
                e = Event(event=event_val, data=obj.get('data'), uid=obj.get('uid', ''))
                e.server_uid = message_uid
                
                # Interceptamos el bloque data crudo (equivalente a json.RawMessage)
                if 'data' in obj:
                    e._raw_data = json.dumps(obj['data'])
                
                ack = MessageState(status=True, server_uid=message_uid, message="ACK recibido Evento", error="", process_status=1)
                ack.send(client)

                import threading
                threading.Thread(target=handle_events, args=(e, client, client.name, int(latency)), daemon=True).start()

            elif msg_type == 2:
                # 🌟 TRASLADO MÁGICO PARA STATE
                state = State(
                    status=obj.get('status', False),
                    message=obj.get('message', ''),
                    error=obj.get('error', ''),
                    data=obj.get('data'),
                    uid=obj.get('uid', '')
                )
                state.server_uid = message_uid
                
                if 'data' in obj:
                    state._raw_data = json.dumps(obj['data'])

                ack = MessageState(status=True, server_uid=message_uid, message="ACK recibido State", error="", process_status=2)
                ack.send(client)

                state_uid = state.uid
                with client.pending_requests_mu:
                    if state_uid in client.pending_requests:
                        req_queue = client.pending_requests.pop(state_uid)
                        req_queue.put(state) # Desbloquea a chan State
                    else:
                        print(f"Proceso no encontrado para State UID: {state_uid}")

            elif msg_type == 3:
                pass # ACKs ignorados

        except Exception as e:
            server_status.put(False)
            return