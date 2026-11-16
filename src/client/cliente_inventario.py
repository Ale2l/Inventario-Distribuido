"""
Cliente de inventario distribuido para gRPC (sin dependencias de interfaz gráfica).

Gestiona el canal de comunicación gRPC, autenticación mediante Bearer token,
traducción de errores de red a excepciones controladas en español y suscripción
a eventos en tiempo real con reconexión automática y retroceso exponencial.
"""

import logging
import threading
import time
from typing import Optional, Callable, List, Tuple
import grpc

from src.generated import inventory_pb2, inventory_pb2_grpc
from src.config import GRPC_TIMEOUT, GRPC_DEFAULT_CLIENT_HOST, GRPC_SERVER_PORT

logger = logging.getLogger("ClienteInventario")


# ==============================================================================
# EXCEPCIONES PROPIAS DEL SISTEMA DISTRIBUIDO
# ==============================================================================

class ErrorInventario(Exception):
    """Excepción base para errores en el cliente de InventarioDistribuido."""
    pass


class ServidorNoDisponible(ErrorInventario):
    """El servidor central gRPC no se encuentra en línea o se cayó la conexión."""
    pass


class TiempoAgotado(ServidorNoDisponible):
    """La solicitud superó el tiempo límite (deadline) configurado."""
    pass


class NoAutenticado(ErrorInventario):
    """Credenciales inválidas, sesión expirada o token ausente."""
    pass


class PermisoDenegado(ErrorInventario):
    """El rol asignado no cuenta con autorización para la operación solicitada."""
    pass


class ErrorRpcGeneral(ErrorInventario):
    """Error genérico devuelto por la llamada remota gRPC."""
    def __init__(self, codigo: grpc.StatusCode, detalle: str):
        self.codigo = codigo
        self.detalle = detalle
        super().__init__(f"Error RPC [{codigo}]: {detalle}")


def _traducir_rpc_error(error: grpc.RpcError) -> ErrorInventario:
    """
    Traduce excepciones crudas de gRPC a excepciones amigables del dominio.

    ¿Por qué funciona así?:
    En sistemas distribuidos, una desconexión o fallo en el servidor no debe
    provocar que la aplicación cliente se cierre de forma abruptly (crash).
    Traducir los errores a tipos específicos permite que las capas superiores
    (GUI o CLI) muestren alertas comprensibles al usuario y ejecuten reintentos.
    """
    codigo = error.code()
    detalles = error.details() or "Sin detalles adicionales."

    if codigo == grpc.StatusCode.UNAVAILABLE:
        return ServidorNoDisponible(
            f"El servidor central no está disponible en la dirección configurada. "
            f"Verifique la conexión de red o que el servidor esté activo. (Detalle: {detalles})"
        )
    elif codigo == grpc.StatusCode.DEADLINE_EXCEEDED:
        return TiempoAgotado(
            f"Tiempo de espera agotado ({GRPC_TIMEOUT}s). El servidor no respondió a tiempo."
        )
    elif codigo == grpc.StatusCode.UNAUTHENTICATED:
        return NoAutenticado(
            f"Acceso no autenticado: {detalles}"
        )
    elif codigo == grpc.StatusCode.PERMISSION_DENIED:
        return PermisoDenegado(
            f"Operación no autorizada para su rol: {detalles}"
        )
    return ErrorRpcGeneral(codigo, detalles)


class ClienteInventario:
    """
    Cliente gRPC desacoplado para interactuar con el servidor central de inventario.
    Seguro para concurrencia y con manejo de reconexión transparente.
    """

    def __init__(self, timeout: float = GRPC_TIMEOUT):
        self.timeout = timeout
        self._canal: Optional[grpc.Channel] = None
        self._stub: Optional[inventory_pb2_grpc.InventoryServiceStub] = None

        self._host: str = GRPC_DEFAULT_CLIENT_HOST
        self._puerto: int = GRPC_SERVER_PORT
        self._token: Optional[str] = None
        self._rol: Optional[str] = None
        self._sede: Optional[str] = None
        self._usuario: Optional[str] = None

        # Control del hilo de streaming
        self._hilo_stream: Optional[threading.Thread] = None
        self._detener_streaming = threading.Event()
        self._stream_llamada = None
        self._lock = threading.Lock()

    @property
    def rol(self) -> Optional[str]:
        """Rol de la cuenta autenticada (SEDE o SIMULADOR)."""
        return self._rol

    @property
    def sede(self) -> Optional[str]:
        """Nombre legible de la sede asociada al usuario autenticado."""
        return self._sede

    @property
    def usuario(self) -> Optional[str]:
        """Nombre de usuario autenticado."""
        return self._usuario

    @property
    def token(self) -> Optional[str]:
        """Token de sesión opaco generado por el servidor central."""
        return self._token

    @property
    def esta_autenticado(self) -> bool:
        """Indica si el cliente posee un token de sesión activo."""
        return self._token is not None

    def _metadata(self) -> Tuple[Tuple[str, str], ...]:
        """Genera la tupla de metadatos gRPC con la cabecera de autorización Bearer."""
        if self._token:
            return (("authorization", f"Bearer {self._token}"),)
        return ()

    def conectar(
        self,
        host: str,
        puerto: int,
        usuario: str,
        password: str,
        timeout: Optional[float] = None,
    ) -> bool:
        """
        Establece el canal gRPC con el servidor central y ejecuta el login para obtener token.

        Retorna True si la autenticación fue exitosa, o False si las credenciales son incorrectas.
        Lanza ServidorNoDisponible o TiempoAgotado ante errores de red.
        """
        limite_tiempo = timeout or self.timeout
        self.cerrar()

        self._host = host.strip()
        self._puerto = int(puerto)
        target = f"{self._host}:{self._puerto}"

        self._canal = grpc.insecure_channel(target)
        self._stub = inventory_pb2_grpc.InventoryServiceStub(self._canal)

        try:
            req = inventory_pb2.LoginRequest(username=usuario.strip(), password=password)
            resp = self._stub.Login(req, timeout=limite_tiempo)
        except grpc.RpcError as e:
            self.cerrar()
            raise _traducir_rpc_error(e) from e

        if not resp.success:
            self.cerrar()
            return False

        self._token = resp.token
        self._rol = resp.rol
        self._sede = resp.sede_nombre
        self._usuario = usuario.strip()
        logger.info(f"Conexión gRPC establecida exitosamente con {target} [{self._sede} / {self._rol}].")
        return True

    def cerrar(self):
        """Cierra ordenadamente la suscripción de eventos y el canal gRPC."""
        self.desuscribir()
        with self._lock:
            if self._canal:
                try:
                    self._canal.close()
                except Exception:
                    pass
                self._canal = None
                self._stub = None
            self._token = None
            self._rol = None
            self._sede = None
            self._usuario = None

    def _verificar_stub(self):
        """Valida que exista un stub activo antes de invocar llamadas remotas."""
        if self._stub is None:
            raise ServidorNoDisponible("El cliente no está conectado a ningún servidor gRPC.")

    # ==========================================================================
    # MÉTODOS RPC DE NEGOCIO
    # ==========================================================================

    def consultar_producto(self, product_id: str) -> inventory_pb2.GetProductResponse:
        """RPC GetProduct: Consulta un producto por su identificador."""
        self._verificar_stub()
        try:
            req = inventory_pb2.GetProductRequest(product_id=product_id.strip())
            return self._stub.GetProduct(req, timeout=self.timeout, metadata=self._metadata())
        except grpc.RpcError as e:
            raise _traducir_rpc_error(e) from e

    def listar_productos(self) -> inventory_pb2.ListProductsResponse:
        """RPC ListProducts: Retorna la lista de todos los productos disponibles."""
        self._verificar_stub()
        try:
            req = inventory_pb2.ListProductsRequest()
            return self._stub.ListProducts(req, timeout=self.timeout, metadata=self._metadata())
        except grpc.RpcError as e:
            raise _traducir_rpc_error(e) from e

    def ajustar_stock(
        self, product_id: str, delta: int, expected_version: int
    ) -> inventory_pb2.AdjustStockResponse:
        """RPC AdjustStock: Aumenta o disminuye existencias con control optimista de versión."""
        self._verificar_stub()
        try:
            req = inventory_pb2.AdjustStockRequest(
                product_id=product_id.strip(),
                delta=delta,
                expected_version=expected_version,
            )
            return self._stub.AdjustStock(req, timeout=self.timeout, metadata=self._metadata())
        except grpc.RpcError as e:
            raise _traducir_rpc_error(e) from e

    def establecer_stock(
        self, product_id: str, new_stock: int, expected_version: int
    ) -> inventory_pb2.SetStockResponse:
        """RPC SetStock: Fija una cantidad absoluta de stock con control de versión."""
        self._verificar_stub()
        try:
            req = inventory_pb2.SetStockRequest(
                product_id=product_id.strip(),
                new_stock=new_stock,
                expected_version=expected_version,
            )
            return self._stub.SetStock(req, timeout=self.timeout, metadata=self._metadata())
        except grpc.RpcError as e:
            raise _traducir_rpc_error(e) from e

    def crear_producto(
        self, product_id: str, name: str, stock: int
    ) -> inventory_pb2.CreateProductResponse:
        """RPC CreateProduct: Registra un nuevo producto en el catálogo central."""
        self._verificar_stub()
        try:
            req = inventory_pb2.CreateProductRequest(
                product_id=product_id.strip(),
                name=name.strip(),
                stock=stock,
            )
            return self._stub.CreateProduct(req, timeout=self.timeout, metadata=self._metadata())
        except grpc.RpcError as e:
            raise _traducir_rpc_error(e) from e

    def actualizar_producto(
        self, product_id: str, name: str, expected_version: int
    ) -> inventory_pb2.UpdateProductResponse:
        """RPC UpdateProduct: Modifica el nombre de un producto con control de versión."""
        self._verificar_stub()
        try:
            req = inventory_pb2.UpdateProductRequest(
                product_id=product_id.strip(),
                name=name.strip(),
                expected_version=expected_version,
            )
            return self._stub.UpdateProduct(req, timeout=self.timeout, metadata=self._metadata())
        except grpc.RpcError as e:
            raise _traducir_rpc_error(e) from e

    def eliminar_producto(
        self, product_id: str, expected_version: int
    ) -> inventory_pb2.DeleteProductResponse:
        """RPC DeleteProduct: Elimina un producto validando la versión esperada."""
        self._verificar_stub()
        try:
            req = inventory_pb2.DeleteProductRequest(
                product_id=product_id.strip(),
                expected_version=expected_version,
            )
            return self._stub.DeleteProduct(req, timeout=self.timeout, metadata=self._metadata())
        except grpc.RpcError as e:
            raise _traducir_rpc_error(e) from e

    def listar_movimientos(
        self, producto_id: Optional[str] = None, limite: int = 50
    ) -> inventory_pb2.ListMovementsResponse:
        """RPC ListMovements: Consulta el historial inmutable de auditoría."""
        self._verificar_stub()
        try:
            req = inventory_pb2.ListMovementsRequest(
                product_id=producto_id.strip() if producto_id else "",
                limite=limite,
            )
            return self._stub.ListMovements(req, timeout=self.timeout, metadata=self._metadata())
        except grpc.RpcError as e:
            raise _traducir_rpc_error(e) from e

    # ==========================================================================
    # SUSCRIPCIÓN EN VIVO (SERVER STREAMING CON RECONEXIÓN EXPONENCIAL)
    # ==========================================================================

    def suscribir(
        self,
        callback_evento: Callable[[inventory_pb2.ProductEvent], None],
        callback_estado: Optional[Callable[[str, str], None]] = None,
    ):
        """
        Inicia un hilo demonio que consume el flujo de WatchProducts.

        Parámetros:
            callback_evento: Función invocada al recibir cada ProductEvent.
            callback_estado: Función opcional invocada al cambiar el estado
                             ('conectado', 'reconectando', 'desconectado').
        """
        self.desuscribir()
        self._detener_streaming.clear()
        self._hilo_stream = threading.Thread(
            target=self._bucle_streaming,
            args=(callback_evento, callback_estado),
            name="StreamEventosInventario",
            daemon=True,
        )
        self._hilo_stream.start()

    def desuscribir(self):
        """Detiene de forma limpia el flujo de eventos y libera el hilo."""
        self._detener_streaming.set()
        with self._lock:
            if self._stream_llamada:
                try:
                    self._stream_llamada.cancel()
                except Exception:
                    pass
                self._stream_llamada = None

        if self._hilo_stream and self._hilo_stream.is_alive():
            self._hilo_stream.join(timeout=1.5)
            self._hilo_stream = None

    def _bucle_streaming(
        self,
        callback_evento: Callable[[inventory_pb2.ProductEvent], None],
        callback_estado: Optional[Callable[[str, str], None]],
    ):
        """
        Bucle de recepción de eventos con reconexión exponencial automática.

        ¿Por qué funciona así?:
        En redes inalámbricas (Wi-Fi o túneles VPN como Radmin), microcortes de red
        son comunes. Si el stream gRPC se interrumpe, el cliente espera un tiempo
        creciente (1s, 2s, 4s... hasta 10s) para no saturar al servidor caído y
        notifica el estado 'reconectando'. Al restablecerse la red, se reconecta
        automáticamente y reanuda la escucha sin intervención del usuario.
        """
        espera_reintento = 1.0
        max_espera = 10.0

        while not self._detener_streaming.is_set():
            if not self._stub:
                time.sleep(1.0)
                continue

            try:
                if callback_estado:
                    callback_estado("conectado", "Conectando al flujo de eventos...")

                req = inventory_pb2.WatchRequest(client_id=self._usuario or "cliente_gui")
                # Llamada streaming persistente
                with self._lock:
                    self._stream_llamada = self._stub.WatchProducts(req, metadata=self._metadata())

                if callback_estado:
                    callback_estado("conectado", "Escuchando eventos en tiempo real.")

                espera_reintento = 1.0  # Reiniciar espera tras conexión exitosa

                for evento in self._stream_llamada:
                    if self._detener_streaming.is_set():
                        break
                    try:
                        callback_evento(evento)
                    except Exception as err_cb:
                        logger.error(f"Error en callback de evento de usuario: {err_cb}")

            except grpc.RpcError as rpc_err:
                if self._detener_streaming.is_set():
                    break
                err_traducido = _traducir_rpc_error(rpc_err)
                logger.warning(f"Corte en streaming de eventos: {err_traducido}")
                if callback_estado:
                    callback_estado(
                        "reconectando",
                        f"{err_traducido}. Reintentando conexión en {int(espera_reintento)}s...",
                    )
                time.sleep(espera_reintento)
                espera_reintento = min(espera_reintento * 2, max_espera)

            except Exception as e:
                if self._detener_streaming.is_set():
                    break
                logger.warning(f"Excepción en bucle de streaming: {e}")
                if callback_estado:
                    callback_estado(
                        "reconectando",
                        f"Error de conexión: {e}. Reintentando en {int(espera_reintento)}s...",
                    )
                time.sleep(espera_reintento)
                espera_reintento = min(espera_reintento * 2, max_espera)

        if callback_estado:
            callback_estado("desconectado", "Flujo de eventos en tiempo real detenido.")
