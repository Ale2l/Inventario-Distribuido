"""
Bus de eventos en tiempo real para InventarioDistribuido.

Implementa un patrón publicador-suscriptor thread-safe en memoria.
Cada cliente conectado al streaming gRPC (WatchProducts) recibe su propia
cola FIFO independiente para recibir eventos de creación, actualización y
eliminación de productos sin retardos de sondeo (polling).
"""

import logging
import queue
import threading
from typing import Set

from src.generated import inventory_pb2

logger = logging.getLogger("BusEventos")


class BusEventos:
    """
    Bus de eventos en memoria seguro para hilos.
    Distribuye copias de ProductEvent a todos los suscriptores activos.
    """

    def __init__(self, maxsize_cola: int = 100):
        self._lock = threading.Lock()
        self._suscriptores: Set[queue.Queue] = set()
        self._maxsize_cola = maxsize_cola

    def suscribir(self) -> queue.Queue:
        """
        Crea y registra una nueva cola de eventos para un suscriptor.
        Retorna la cola desde la cual el suscriptor consumirá eventos.
        """
        q = queue.Queue(maxsize=self._maxsize_cola)
        with self._lock:
            self._suscriptores.add(q)
            num_actual = len(self._suscriptores)
        logger.info(f"[+] Nuevo suscriptor registrado en bus de eventos. Total activos: {num_actual}")
        return q

    def desuscribir(self, q: queue.Queue) -> None:
        """
        Elimina la cola de un suscriptor desconectado para evitar fugas de memoria.
        """
        with self._lock:
            self._suscriptores.discard(q)
            num_actual = len(self._suscriptores)
        logger.info(f"[-] Suscriptor eliminado del bus de eventos. Total activos: {num_actual}")

    def publicar(self, evento: inventory_pb2.ProductEvent) -> None:
        """
        Publica un evento a todas las colas de suscriptores activos.

        ¿Por qué funciona así?:
        Al copiar la lista de suscriptores bajo el lock y luego encolar sin bloquear
        (put_nowait), un cliente lento o con red saturada no degrada ni bloquea a los
        demás clientes ni a las operaciones de escritura del servidor central.
        """
        with self._lock:
            suscriptores = list(self._suscriptores)

        for q in suscriptores:
            try:
                q.put_nowait(evento)
            except queue.Full:
                logger.warning("Cola de suscriptor saturada. Descartando evento para evitar bloqueo.")

