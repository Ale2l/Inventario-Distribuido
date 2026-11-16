"""
Punto de entrada del servidor gRPC de InventarioDistribuido.
Inicia el servidor en el puerto TCP 50051 con un ThreadPoolExecutor de 20 hilos
para soportar múltiples clientes concurrentes y conexiones de Server Streaming.
"""

import logging
import sys
from concurrent import futures
from typing import Optional
import grpc

from src.generated import inventory_pb2_grpc
from src.server.servicio import InventoryServiceServicer
from src.server.interceptor import GestorSesiones, AuthInterceptor
from src.server.bus_eventos import BusEventos
from src.config import GRPC_SERVER_PORT

DEFAULT_PORT = GRPC_SERVER_PORT
DEFAULT_MAX_WORKERS = 20


def crear_servidor(
    port: int = DEFAULT_PORT,
    max_workers: int = DEFAULT_MAX_WORKERS,
    almacen=None,
    gestor_sesiones: Optional[GestorSesiones] = None,
    bus_eventos: Optional[BusEventos] = None,
    usar_interceptor: bool = False,
    callback_auditoria=None,
) -> grpc.Server:
    """
    Crea y configura la instancia del servidor gRPC con su servicio e interceptores registrados.
    Reutilizable tanto por el punto de entrada principal como por la GUI y suites de pruebas.
    """
    sesiones = gestor_sesiones if gestor_sesiones is not None else GestorSesiones()
    eventos = bus_eventos if bus_eventos is not None else BusEventos()

    interceptores = []
    if usar_interceptor:
        interceptores.append(AuthInterceptor(gestor_sesiones=sesiones, almacen=almacen))

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=max_workers),
        interceptors=interceptores if interceptores else None,
    )

    servicer = InventoryServiceServicer(
        almacen=almacen,
        gestor_sesiones=sesiones,
        bus_eventos=eventos,
        callback_auditoria=callback_auditoria,
    )
    inventory_pb2_grpc.add_InventoryServiceServicer_to_server(servicer, server)

    direccion = f"0.0.0.0:{port}"
    try:
        puerto_asignado = server.add_insecure_port(direccion)
        if puerto_asignado == 0:
            raise RuntimeError(
                f"No se pudo vincular al puerto TCP {port}. "
                f"El puerto ya se encuentra en uso por otro proceso o no se tienen permisos."
            )
    except Exception as e:
        raise RuntimeError(
            f"No se pudo enlazar al puerto TCP {port}. "
            f"El puerto probablemente ya está ocupado por otra instancia o proceso. Detalle: {e}"
        ) from e

    # Atributos de utilidad expuestos en la instancia
    server.bound_port = puerto_asignado
    server.servicer = servicer
    server.gestor_sesiones = sesiones
    server.bus_eventos = eventos

    return server


def main():
    """Función principal para arrancar el servidor gRPC desde línea de comandos."""
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger("ServidorInventario")

    logger.info("Inicializando servidor central de InventarioDistribuido...")

    try:
        server = crear_servidor(port=DEFAULT_PORT, max_workers=DEFAULT_MAX_WORKERS)
    except RuntimeError as err:
        logger.error(f"Fallo crítico al iniciar el servidor: {err}")
        sys.exit(1)

    server.start()
    logger.info(
        f"[OK] Servidor gRPC escuchando exitosamente en el puerto TCP {DEFAULT_PORT} (0.0.0.0:{DEFAULT_PORT})."
    )
    logger.info(f"[OK] ThreadPoolExecutor activo con hasta {DEFAULT_MAX_WORKERS} hilos concurrentes.")
    logger.info("Presiona Ctrl+C para detener el servidor.")

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("\nInterrupción detectada (Ctrl+C). Deteniendo servidor gRPC...")
        server.stop(grace=2)
        logger.info("Servidor detenido correctamente.")


if __name__ == "__main__":
    main()
