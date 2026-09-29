# SUBNX para Python

Driver de Python de **SUBNX**: un balanceador/orquestador tipo pub/sub donde varios
nodos que se suscriben al mismo evento forman un _grid_ (un servidor distribuido en
red) y el balanceador decide cuál atiende cada petición.

- Servidor SUBNX: se distribuye como ejecutable y se corre aparte.
- Driver de Go: [`github.com/HManuelCC/subnx-go`](https://github.com/HManuelCC/subnx-go)

## Instalación

Ya está publicado en PyPI:

```bash
pip install subnx
```

```python
from subnx import Client, GlobalEvents, Event, State
```

## Ejemplos

```
examples/cmd/worker.py    nodo del grid: registra eventos y responde (sin HTTP)
examples/cmd/gateway.py   API HTTP que traduce peticiones a eventos SUBNX
examples/cmd/sender.py    uso programático: síncrono, timeout, errores, ráfaga, tarea periódica
```

Los ejemplos viven en el repositorio (no vienen dentro del paquete de PyPI), así que
para correrlos hay que clonarlo. El driver se instala aparte con `pip install subnx`:

```bash
git clone https://github.com/HManuelCC/subnx-python.git
cd subnx-python
pip install subnx
```

Con el servidor SUBNX corriendo (los ejemplos asumen `localhost:9000`, api key
`API_KEY`, sin TLS; todos aceptan `--host --port --name --key --tls`):

```bash
# 3 workers con distinto nombre y el mismo evento "login" => un solo grid
python examples/cmd/worker.py --name worker-1
python examples/cmd/worker.py --name worker-2
python examples/cmd/worker.py --name worker-3

# Escenarios programáticos (imprime cuántas peticiones atendió cada worker)
python examples/cmd/sender.py

# API HTTP (opcional)
python examples/cmd/gateway.py
```

```bash
curl -X POST localhost:8080/login -d '{"username":"manuel","password":"1234"}'    # 200
curl -X POST localhost:8080/login -d '{"username":"","password":""}'              # 400 (status=False)
curl -X POST localhost:8080/telemetry -d '{"cpu_usage":"45%","ram":"2GB"}'        # 202 fire-and-forget
curl 'localhost:8080/reporte?seconds=3&timeout=1'                                  # 504 timeout
curl localhost:8080/fallo                                                          # 400
```

## Cómo se usa el driver

**Worker: se registran los eventos antes de conectar**

```python
from dataclasses import dataclass
from subnx import Client, GlobalEvents, Event, State

@dataclass
class Credentials:
    username: str = ""
    password: str = ""

def on_login(event: Event, conn: Client):
    creds = Credentials()
    try:
        event.bind(creds)
    except Exception:
        event.reply(conn, State(status=False, error="INVALID_PAYLOAD"))
        return
    event.reply(conn, State(status=True, message="ok", data={"user": creds.username}))

GlobalEvents.add_event("login", on_login)

client = Client("host", "port", "client_name", "client_api_key", False)
client.listen()  # bloquea; apaga limpio con Ctrl+C
```

**Cliente: enviar un evento y esperar la respuesta**

```python
def on_reply(resp: State):
    print(resp.message)  # solo se ejecuta con status=True

try:
    client.send(
        Event(event="login", data={"username": "manuel", "password": "1234"}),
        timeout=5.0,  # segundos; si lo omites se usa el default
        callback=on_reply,
    )
except Exception as exc:
    # status=False del worker, timeout o error de red
    print(exc)
```

## Bind: tus tipos son el contrato

En SUBNX no hay un esquema central ni un contrato declarado en el servidor. El
**tipo que tú pasas a `bind` es el contrato**: si sus atributos coinciden con los
datos que se enviaron, se rellena; si no son compatibles, `bind` falla.

- Solo importan los **nombres de los atributos**, no el de la clase.
- Existe en los dos lados: `event.bind(...)` al **recibir** una petición y
  `state.bind(...)` al **leer la respuesta** (`State.data`).
- Siempre maneja el error: un `bind` incompatible lanza excepción.

```python
@dataclass
class LoginReply:
    token: str = ""
    handled_by: str = ""

def on_reply(resp: State):
    out = LoginReply()
    resp.bind(out)                  # mismo mecanismo que event.bind
    print(out.token, out.handled_by)
```

Si el emisor mandó `{"username": "Humberto", "password": "1244"}`, cualquier clase
con atributos `username` y `password` sirve como receptor.

## Reglas del SDK

| Tema           | Comportamiento                                                                                                                                    |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| Orden          | `GlobalEvents.add_event(...)` **antes** de crear el `Client(...)`                                                                                 |
| Conexión       | `Client(...)` se devuelve de inmediato; el handshake ocurre en segundo plano (los errores salen en consola). Por ahora se espera con `time.sleep` |
| `send`         | Bloquea hasta que responde un worker o vence el timeout                                                                                           |
| `status=True`  | Se ejecuta el callback y `send` regresa normal                                                                                                    |
| `status=False` | **No** se ejecuta el callback; `send` lanza excepción                                                                                             |
| Sin callback   | Fire-and-forget: `send` regresa en cuanto escribe el paquete                                                                                      |
| Datos          | Sin contrato central: `bind` rellena el tipo que pases con los atributos que coincidan                                                            |
| `State.data`   | La librería lo serializa. En el cliente se lee con `state.bind(obj)`                                                                              |
| Grid           | Varios clientes con distinto nombre y el mismo `add_event` comparten la carga                                                                     |
