# subnx/handlers.py
import json
import threading
import time
from typing import TYPE_CHECKING

from .events import GlobalEvents

if TYPE_CHECKING:
    from .client import Client
    from .models import Event

def handle_events(e: 'Event', conn: 'Client', client_name: str, latency: int):
    if e.event == "connect":
        handle_connect_event(e, conn, client_name, latency)
        return

    handler, exists = GlobalEvents.get_handler(e.event)
    if not exists:
        print(f"Evento no reconocido: {e.server_uid} {e.event}")
        from .models import State
        e.reply(conn, State(status=False, message="Evento no reconocido", error="NOT_FOUND"))
        return

    # 🐕 EL PERRO GUARDIÁN DE DOS FASES
    def guard_dog():
        if e.replied.wait(timeout=0.1): 
            return
            
        e.extended.set() # Activar prórroga
        
        from .models import MessageState
        keep_alive = MessageState(status=True, server_uid=e.server_uid, message="El worker está trabajando", error="", process_status=1)
        keep_alive.send(conn) # 🌟 Método directo del objeto

        if not e.replied.wait(timeout=28.0):
            abort = MessageState(status=False, server_uid=e.server_uid, message="Worker colgado", error="WORKER_HANG", process_status=2)
            abort.send(conn)

    threading.Thread(target=guard_dog, daemon=True).start()

    # 🚀 Ejecutamos al desarrollador
    try:
        handler(e, conn)
    except Exception as ex:
        print(f"Error en el handler del desarrollador: {ex}")

def handle_connect_event(e: 'Event', conn: 'Client', client_name: str, latency: int):
    from .stats import get_system_stats
    from .models import ClientHardwareResourcesStatistics, ClientInformation, EventsSubscribed, State
    
    stats = ClientHardwareResourcesStatistics()
    try:
        get_system_stats(stats)
    except Exception as err:
        print(f"Error obteniendo estadísticas del sistema: {err}")
        return

    api_key_str = conn.api_key if conn.api_key else ""

    info = ClientInformation(
        client_name=client_name,
        api_key=api_key_str,
        latency=float(latency),
        resources=stats,
        events=EventsSubscribed(GlobalEvents.get_registered_event_names())
    )

    try:
        json_data = json.dumps(info.to_dict())
        e.reply(conn, State(status=True, message="Cliente conectado con exito.", data=json_data))
        conn.set_ready()
    except Exception as err:
        e.reply(conn, State(status=False, message="Error interno de JSON", error=str(err)))