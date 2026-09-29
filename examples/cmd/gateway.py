"""API GATEWAY (HTTP -> SUBNX)

Traduce peticiones HTTP a eventos SUBNX. send() bloquea hasta que un worker
contesta o vence el timeout, y el callback corre en el mismo hilo que send(),
así que al volver de send() ya tenemos la respuesta para escribirla al navegador.
ThreadingHTTPServer atiende cada petición en su propio hilo.

Contrato de errores del SDK:
  - Respuesta con status=True  -> se ejecuta el callback y send() regresa normal
  - Respuesta con status=False -> NO se ejecuta el callback y send() lanza excepción
  - Sin respuesta a tiempo     -> send() lanza excepción de timeout

    python examples/cmd/gateway.py

    curl -X POST localhost:8080/login -d '{"username":"manuel","password":"1234"}'
    curl -X POST localhost:8080/login -d '{"username":"","password":""}'      # status=False
    curl -X POST localhost:8080/telemetry -d '{"cpu_usage":"45%","ram":"2GB"}' # fire-and-forget
    curl 'localhost:8080/reporte?seconds=3&timeout=1'                          # timeout
    curl localhost:8080/fallo                                                  # status=False
"""

import argparse
import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional
from urllib.parse import parse_qs, urlparse

from subnx import Client, Event, State

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("gateway")


class Gateway(BaseHTTPRequestHandler):
    client: Client = None  # se inyecta en main()

    # ------------------------------------------------------------------
    # Rutas
    # ------------------------------------------------------------------
    def do_POST(self) -> None:
        path = urlparse(self.path).path

        if path == "/login":
            body = self.read_json()
            if body is None:
                return
            # Credenciales en el body, nunca en la query string.
            self.forward(Event(event="login", data=body))

        elif path == "/telemetry":
            body = self.read_json()
            if body is None:
                return
            try:
                # Sin callback => fire-and-forget: send() regresa al escribir el paquete.
                self.client.send(Event(event="telemetria", data=body))
            except Exception as exc:
                self.write_json(502, {"status": False, "error": str(exc)})
                return
            self.write_json(202, {"status": True, "message": "encolado"})

        else:
            self.write_json(404, {"status": False, "error": "ruta no encontrada"})

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)

        if parsed.path == "/reporte":
            seconds = to_int(query.get("seconds", ["1"])[0], default=1)

            # Timeout personalizado en segundos (float). Sin él se usa el default del SDK.
            timeout = to_int(query.get("timeout", [""])[0], default=0)
            self.forward(
                Event(event="reporte_lento", data={"seconds": seconds}),
                timeout=float(timeout) if timeout > 0 else None,
            )

        elif parsed.path == "/fallo":
            self.forward(Event(event="fallo", data={}))

        else:
            self.write_json(404, {"status": False, "error": "ruta no encontrada"})

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def forward(self, event: Event, timeout: Optional[float] = None) -> None:
        """Envía el evento y devuelve al navegador lo que contestó el worker."""
        result = {}

        def on_reply(state: State) -> None:
            # Solo se llega aquí con status=True.
            result["state"] = state

        kwargs = {"callback": on_reply}
        if timeout is not None:
            kwargs["timeout"] = timeout

        try:
            self.client.send(event, **kwargs)
        except Exception as exc:
            self.write_json(status_from_error(exc), {"status": False, "error": str(exc)})
            return

        state = result.get("state")
        if state is None:
            self.write_json(502, {"status": False, "error": "sin respuesta del backend"})
            return
        self.write_json(200, state.to_dict())

    def read_json(self) -> Optional[dict]:
        try:
            length = int(self.headers.get("Content-Length", 0))
            return json.loads(self.rfile.read(length) or b"{}")
        except (ValueError, json.JSONDecodeError):
            self.write_json(400, {"status": False, "error": "JSON inválido"})
            return None

    def write_json(self, code: int, payload: dict) -> None:
        raw = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, format, *args) -> None:  # silencia el log por request
        pass


def to_int(value: str, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def status_from_error(exc: Exception) -> int:
    """Traduce la excepción del SDK a un código HTTP."""
    msg = str(exc).lower()
    if isinstance(exc, TimeoutError) or "timeout" in msg:
        return 504  # no contestó a tiempo
    if "error del servidor" in msg:
        return 400  # el worker respondió status=False
    return 502      # no se pudo hablar con el servidor, etc.


def main() -> None:
    parser = argparse.ArgumentParser(description="API gateway SUBNX")
    parser.add_argument("--host", default="localhost", help="host del servidor SUBNX")
    parser.add_argument("--port", default="9000", help="puerto del servidor SUBNX")
    parser.add_argument("--name", default="PYTHON_API", help="nombre de este cliente")
    parser.add_argument("--key", default="API_KEY", help="api key del cliente")
    parser.add_argument("--tls", action="store_true", help="conectar con TLS")
    parser.add_argument("--http-port", type=int, default=8080, help="puerto del servidor HTTP")
    args = parser.parse_args()

    # El gateway solo emite eventos, no registra ninguno con add_event.
    client = Client(args.host, args.port, args.name, args.key, args.tls)
    Gateway.client = client

    server = ThreadingHTTPServer(("0.0.0.0", args.http_port), Gateway)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    log.info("[API] escuchando en :%d", args.http_port)

    # Bloquea hasta Ctrl+C; luego cerramos HTTP.
    client.listen()
    server.shutdown()
    log.info("[API] apagado limpio")


if __name__ == "__main__":
    main()