# subnx/events.py
import threading
from typing import Callable, List, Tuple, Optional, Dict, TYPE_CHECKING

if TYPE_CHECKING:
    from .client import Client
    from .models import Event

# EventHandler define la firma limpia para el desarrollador
EventHandler = Callable[['Event', 'Client'], None]

class EventRouter:
    def __init__(self):
        self._mu = threading.Lock()
        self.handlers: Dict[str, EventHandler] = {}

    def add_event(self, event_name: str, handler: EventHandler) -> None:
        with self._mu:
            self.handlers[event_name] = handler

    def remove_event(self, event_name: str) -> None:
        with self._mu:
            if event_name in self.handlers:
                del self.handlers[event_name]

    def get_registered_event_names(self) -> List[str]:
        with self._mu:
            return list(self.handlers.keys())

    def get_handler(self, event_name: str) -> Tuple[Optional[EventHandler], bool]:
        with self._mu:
            handler = self.handlers.get(event_name)
            exists = handler is not None
            return handler, exists

# GlobalEvents es el enrutador principal del SDK
GlobalEvents = EventRouter()