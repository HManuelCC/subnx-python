"""SENDER (uso programático, sin HTTP)

Recorre los escenarios del SDK desde código:
  1. Envío síncrono con respuesta y lectura de Data con bind
  2. Timeout personalizado
  3. Respuesta con status=False (send lanza excepción)
  4. Fire-and-forget
  5. Ráfaga concurrente para ver el balanceo entre workers
  6. Tarea periódica

Levanta antes 2 o 3 workers y luego:

    python examples/cmd/sender.py
"""

import argparse
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from subnx import Client, Event, State


# "Contrato" de lo que esperamos leer de la respuesta (State.bind).
# Solo importan los nombres de los atributos, no el de la clase.
@dataclass
class LoginReply:
    token: str = ""
    handled_by: str = ""


def section(title: str) -> None:
    line = "─" * 46
    print(f"\n{line}\n{title}\n{line}")


# 1 ---------------------------------------------------------------------------
def sync_login(client: Client) -> None:
    event = Event(event="login", data={"username": "manuel", "password": "supersecretpassword"})

    def on_reply(resp: State) -> None:
        print(f"✅ {resp.message}")

        # Igual que event.bind: el tipo que pasas es el contrato.
        out = LoginReply()
        try:
            resp.bind(out)
        except Exception as exc:
            print("   ⚠️ no pude leer Data:", exc)
            return
        print(f"   token={out.token} atendido por={out.handled_by}")

    try:
        # Sin timeout => default del SDK. send() bloquea hasta que responde un worker.
        client.send(event, callback=on_reply)
    except Exception as exc:
        print("❌", exc)


# 2 ---------------------------------------------------------------------------
def custom_timeout(client: Client) -> None:
    start = time.time()
    try:
        client.send(
            Event(event="reporte_lento", data={"seconds": 3}),
            timeout=1.0,  # segundos
            callback=lambda resp: print("✅ (no debería llegar aquí):", resp.message),
        )
    except Exception as exc:
        print(f"❌ esperado, tras {time.time() - start:.1f}s: {exc}")


# 3 ---------------------------------------------------------------------------
def failing_event(client: Client) -> None:
    try:
        client.send(
            Event(event="fallo", data={}),
            callback=lambda resp: print("✅ (no debería llegar aquí)"),
        )
    except Exception as exc:
        print("❌ esperado:", exc)

    # Otro caso: validación de negocio en el worker (usuario vacío).
    try:
        client.send(
            Event(event="login", data={"username": "", "password": ""}),
            callback=lambda resp: None,
        )
    except Exception as exc:
        print("❌ esperado:", exc)


# 4 ---------------------------------------------------------------------------
def fire_and_forget(client: Client) -> None:
    # Sin callback: regresa apenas escribe el paquete.
    client.send(Event(event="telemetria", data={"cpu_usage": "45%", "ram": "2GB"}))
    print("📤 telemetría enviada sin esperar respuesta")


# 5 ---------------------------------------------------------------------------
def concurrent_burst(client: Client, n: int) -> None:
    counts: Counter = Counter()
    errors = 0
    lock = threading.Lock()

    def one(i: int) -> None:
        nonlocal errors

        def on_reply(resp: State) -> None:
            out = LoginReply()
            try:
                resp.bind(out)
            except Exception:
                pass
            with lock:
                counts[out.handled_by] += 1

        try:
            client.send(
                Event(event="login", data={"username": f"user{i:02d}", "password": "pw"}),
                callback=on_reply,
            )
        except Exception:
            with lock:
                errors += 1

    start = time.time()
    with ThreadPoolExecutor(max_workers=n) as pool:
        list(pool.map(one, range(n)))

    print(f"⏱️  {n} peticiones en {time.time() - start:.2f}s, errores={errors}")
    for worker, total in sorted(counts.items()):
        print(f"   {worker:<12} atendió {total}")


# 6 ---------------------------------------------------------------------------
def periodic(client: Client) -> None:
    for i in range(1, 4):
        time.sleep(3)
        print(f"⏱️  ping {i}...")
        try:
            # El default i=i fija el valor del ciclo dentro del callback.
            client.send(
                Event(event="login", data={"username": f"cron{i}", "password": "pw"}),
                callback=lambda resp, i=i: print(f"🟢 ping {i} respondido: {resp.message}"),
            )
        except Exception as exc:
            print(f"🔴 ping {i} falló: {exc}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Sender SUBNX")
    parser.add_argument("--host", default="localhost", help="host del servidor SUBNX")
    parser.add_argument("--port", default="9000", help="puerto del servidor SUBNX")
    parser.add_argument("--name", default="PY_SENDER", help="nombre de este cliente")
    parser.add_argument("--key", default="API_KEY", help="api key del cliente")
    parser.add_argument("--tls", action="store_true", help="conectar con TLS")
    parser.add_argument("--burst", type=int, default=30, help="peticiones concurrentes en la ráfaga")
    args = parser.parse_args()

    client = Client(args.host, args.port, args.name, args.key, args.tls)

    # Sin OnConnect todavía: margen para el handshake.
    time.sleep(2)

    section("1) Envío síncrono y lectura de Data")
    sync_login(client)

    section("2) Timeout personalizado (reporte de 3s con timeout de 1s)")
    custom_timeout(client)

    section("3) status=False => send lanza excepción, el callback NO corre")
    failing_event(client)

    section("4) Fire-and-forget (sin callback)")
    fire_and_forget(client)

    section(f"5) Ráfaga de {args.burst} logins concurrentes (balanceo del grid)")
    concurrent_burst(client, args.burst)

    section("6) Tarea periódica (3 pings cada 3s)")
    periodic(client)

    print("\n✅ Demo terminada")


if __name__ == "__main__":
    main()