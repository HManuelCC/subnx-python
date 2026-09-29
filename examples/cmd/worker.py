"""WORKER (nodo del grid, sin HTTP)

Regla clave del SDK:
  1. Primero se registran los eventos con GlobalEvents.add_event(...)
  2. Después se crea el cliente con Client(...)
  3. listen() bloquea el hilo principal y apaga limpio con Ctrl+C

Levanta varios workers con distinto --name: todos los que registran el mismo
evento ("login") forman un grid y el balanceador decide a cuál mandárselo.

    python examples/cmd/worker.py --name worker-1
    python examples/cmd/worker.py --name worker-2
    python examples/cmd/worker.py --name worker-3
"""

import argparse
import logging
import threading
import time
from dataclasses import dataclass

from subnx import Client, Event, GlobalEvents, State

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("worker")


# ---------------------------------------------------------------------------
# "Contratos" con bind
# ---------------------------------------------------------------------------
# No hay contrato central: cada receptor declara el tipo que espera y bind()
# lo rellena con los atributos que coincidan. El nombre de la clase no importa,
# solo los nombres de los atributos.
@dataclass
class Credentials:
    username: str = ""
    password: str = ""


@dataclass
class Metrics:
    cpu_usage: str = ""
    ram: str = ""


@dataclass
class ReportRequest:
    seconds: int = 0


def register_events(worker_name: str) -> None:
    handled = 0
    lock = threading.Lock()  # los handlers pueden ejecutarse en paralelo

    # ------------------------------------------------------------------
    # EVENTO CON RESPUESTA: "login"
    # ------------------------------------------------------------------
    def on_login(event: Event, conn: Client) -> None:
        nonlocal handled

        creds = Credentials()
        try:
            event.bind(creds)
        except Exception as exc:
            log.info("[%s] payload inválido: %s", worker_name, exc)
            event.reply(conn, State(status=False, message="Error al parsear datos de login", error="INVALID_PAYLOAD"))
            return

        with lock:
            handled += 1
            n = handled
        log.info("[%s] login #%d user=%r ServerUID=%s", worker_name, n, creds.username, event.server_uid)

        # Validación de negocio: status=False => en el cliente send() falla
        # y el callback NO se ejecuta.
        if not creds.username.strip() or not creds.password:
            event.reply(conn, State(status=False, message="Credenciales incompletas", error="INVALID_CREDENTIALS"))
            return

        time.sleep(0.15)  # simula consulta a BD

        # Incluimos quién atendió para ver el balanceo desde el cliente.
        event.reply(conn, State(
            status=True,
            message="Login validado",
            data={
                "username": creds.username,
                "token": f"tok_{creds.username}_{time.strftime('%H%M%S')}",
                "handled_by": worker_name,
            },
        ))

    # ------------------------------------------------------------------
    # FIRE-AND-FORGET: "telemetria"
    # ------------------------------------------------------------------
    # El emisor no pasó callback, así que aquí no es obligatorio responder.
    def on_telemetria(event: Event, conn: Client) -> None:
        metrics = Metrics()
        try:
            event.bind(metrics)
        except Exception as exc:
            log.info("[%s] telemetría ilegible: %s", worker_name, exc)
            return
        log.info("[%s] telemetría UID=%s cpu=%s ram=%s", worker_name, event.uid, metrics.cpu_usage, metrics.ram)

    # ------------------------------------------------------------------
    # EVENTO LENTO: "reporte_lento" (para demostrar timeouts)
    # ------------------------------------------------------------------
    def on_reporte_lento(event: Event, conn: Client) -> None:
        req = ReportRequest()
        try:
            event.bind(req)
        except Exception:
            req.seconds = 0
        if req.seconds <= 0:
            event.reply(conn, State(status=False, message="seconds inválido", error="INVALID_PAYLOAD"))
            return

        log.info("[%s] generando reporte de %ds...", worker_name, req.seconds)
        time.sleep(req.seconds)
        event.reply(conn, State(
            status=True,
            message="Reporte listo",
            data={"handled_by": worker_name, "seconds": req.seconds},
        ))

    # ------------------------------------------------------------------
    # EVENTO QUE SIEMPRE FALLA: "fallo" (para demostrar status=False)
    # ------------------------------------------------------------------
    def on_fallo(event: Event, conn: Client) -> None:
        event.reply(conn, State(status=False, message="Fallo simulado", error="SIMULATED_FAILURE"))

    GlobalEvents.add_event("login", on_login)
    GlobalEvents.add_event("telemetria", on_telemetria)
    GlobalEvents.add_event("reporte_lento", on_reporte_lento)
    GlobalEvents.add_event("fallo", on_fallo)


def main() -> None:
    parser = argparse.ArgumentParser(description="Worker SUBNX")
    parser.add_argument("--host", default="localhost", help="host del servidor SUBNX")
    parser.add_argument("--port", default="9000", help="puerto del servidor SUBNX")
    parser.add_argument("--name", default="worker-1", help="nombre único de este cliente en el grid")
    parser.add_argument("--key", default="API_KEY", help="api key del cliente")
    parser.add_argument("--tls", action="store_true", help="conectar con TLS")
    args = parser.parse_args()

    # 1) Eventos ANTES de la conexión.
    register_events(args.name)

    # 2) Conexión. El handshake ocurre en segundo plano y los errores salen en consola.
    log.info("[%s] conectando a %s:%s (tls=%s)...", args.name, args.host, args.port, args.tls)
    client = Client(args.host, args.port, args.name, args.key, args.tls)

    # 3) Bloquea (0% CPU) hasta Ctrl+C.
    client.listen()
    log.info("[%s] apagado limpio", args.name)


if __name__ == "__main__":
    main()