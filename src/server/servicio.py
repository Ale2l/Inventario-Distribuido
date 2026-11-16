"""
Implementación completa del servicio gRPC InventoryService.

Conecta todos los métodos RPC del contrato inventory.proto con la capa de
persistencia (PostgreSQL / Almacén), valida control de concurrencia optimista,
distribuye eventos en tiempo real mediante Server Streaming y mantiene
un registro inmutable de auditoría para cada operación.
"""

import logging
import time
from typing import Optional, Dict, Any

from src.generated import inventory_pb2, inventory_pb2_grpc
from src.server.interceptor import GestorSesiones
from src.server.bus_eventos import BusEventos
from src.storage.inventario_memoria import Producto

logger = logging.getLogger("InventoryService")


def _producto_a_proto(p: Optional[Producto]) -> Optional[inventory_pb2.Product]:
    """Convierte un modelo Producto al mensaje protobuf Product."""
    if p is None:
        return None
    return inventory_pb2.Product(
        id=p.id,
        name=p.name,
        stock=p.stock,
        version=p.version,
    )


class InventoryServiceServicer(inventory_pb2_grpc.InventoryServiceServicer):
    """
    Implementación del servicer gRPC InventoryService.
    Thread-safe, desacoplado de la persistencia subyacente.
    """

    def __init__(
        self,
        almacen=None,
        gestor_sesiones: Optional[GestorSesiones] = None,
        bus_eventos: Optional[BusEventos] = None,
        callback_auditoria: Optional[Any] = None,
    ):
        if almacen is not None:
            self.almacen = almacen
        else:
            try:
                from src.storage.inventario_postgres import InventarioPostgres
                self.almacen = InventarioPostgres()
            except Exception as e:
                logger.warning(
                    f"PostgreSQL no disponible ({e}). Utilizando almacén en memoria como respaldo."
                )
                from src.storage.inventario_memoria import InventarioMemoria
                self.almacen = InventarioMemoria()

        self.gestor_sesiones = gestor_sesiones if gestor_sesiones is not None else GestorSesiones()
        self.bus_eventos = bus_eventos if bus_eventos is not None else BusEventos()
        self.callback_auditoria = callback_auditoria

    def _obtener_emisor(self, context) -> Dict[str, str]:
        """Obtiene la identidad del usuario y sede autenticada desde el contexto gRPC."""
        if context is not None and hasattr(context, "sesion_usuario"):
            sesion = getattr(context, "sesion_usuario", None)
            if isinstance(sesion, dict):
                return sesion
        return {
            "username": "sistema",
            "sede_nombre": "Servidor Central",
            "rol": "SEDE",
        }

    def _registrar_auditoria(
        self,
        emisor: Dict[str, str],
        operacion: str,
        producto_id: str,
        detalle: str,
        exito: bool,
        mensaje: str = "",
    ):
        """Registra una operación en la tabla inmutable de auditoría."""
        if hasattr(self.almacen, "registrar_movimiento"):
            try:
                self.almacen.registrar_movimiento(
                    usuario=emisor["username"],
                    sede=emisor["sede_nombre"],
                    operacion=operacion,
                    producto_id=producto_id,
                    detalle=detalle,
                    exito=exito,
                    mensaje=mensaje,
                )
            except Exception as e:
                logger.error(f"Error al registrar movimiento de auditoría: {e}")

        if self.callback_auditoria is not None:
            try:
                self.callback_auditoria({
                    "hora": time.strftime("%H:%M:%S"),
                    "usuario": emisor.get("username", "sistema"),
                    "sede": emisor.get("sede_nombre", "Servidor Central"),
                    "operacion": operacion,
                    "producto_id": producto_id or "-",
                    "detalle": detalle,
                    "exito": exito,
                    "mensaje": mensaje,
                })
            except Exception as e:
                logger.error(f"Error al despachar callback de auditoría: {e}")

    # ==========================================================================
    # 1. AUTENTICACIÓN
    # ==========================================================================

    def Login(self, request, context):
        """Autentica a un usuario y genera un token de sesión opaco."""
        user_info = None
        if hasattr(self.almacen, "autenticar_usuario"):
            user_info = self.almacen.autenticar_usuario(request.username, request.password)

        if user_info is None:
            logger.warning(f"Inicio de sesión fallido para usuario '{request.username}'.")
            self._registrar_auditoria(
                emisor={"username": request.username, "sede_nombre": "Desconocida"},
                operacion="LOGIN",
                producto_id="",
                detalle=f"Intento de autenticación fallido para '{request.username}'",
                exito=False,
                mensaje="Credenciales inválidas.",
            )
            return inventory_pb2.LoginResponse(
                success=False,
                error_message="Usuario o contraseña incorrectos.",
            )

        token = self.gestor_sesiones.crear_sesion(user_info)
        rol_str = user_info["rol"] if isinstance(user_info["rol"], str) else user_info["rol"].value
        sede_nombre = user_info["sede_nombre"]

        logger.info(f"Inicio de sesión exitoso: {request.username} ({sede_nombre}) [Rol: {rol_str}]")
        self._registrar_auditoria(
            emisor=user_info,
            operacion="LOGIN",
            producto_id="",
            detalle=f"Inicio de sesión exitoso desde '{sede_nombre}'",
            exito=True,
            mensaje="Autenticación completada con éxito.",
        )

        return inventory_pb2.LoginResponse(
            success=True,
            error_message="",
            token=token,
            rol=rol_str,
            sede_nombre=sede_nombre,
        )

    # ==========================================================================
    # 2. CONSULTAS DE CATÁLOGO (LECTURA)
    # ==========================================================================

    def GetProduct(self, request, context):
        """Consulta un producto por su identificador único."""
        prod = self.almacen.get_product(request.product_id)
        if prod is None:
            return inventory_pb2.GetProductResponse(
                success=False,
                error_message=f"Producto con identificador '{request.product_id}' no encontrado.",
            )
        return inventory_pb2.GetProductResponse(
            success=True,
            error_message="",
            product=_producto_a_proto(prod),
        )

    def ListProducts(self, request, context):
        """Lista todos los productos disponibles en el inventario."""
        productos = self.almacen.list_products()
        proto_productos = [_producto_a_proto(p) for p in productos]
        return inventory_pb2.ListProductsResponse(
            success=True,
            error_message="",
            products=proto_productos,
        )

    # ==========================================================================
    # 3. MODIFICACIONES DE EXISTENCIAS
    # ==========================================================================

    def AdjustStock(self, request, context):
        """Aumenta o disminuye existencias con control de concurrencia optimista."""
        emisor = self._obtener_emisor(context)
        success, error_msg, conflict, prod = self.almacen.adjust_stock(
            product_id=request.product_id,
            delta=request.delta,
            expected_version=request.expected_version,
        )

        detalle = f"Ajuste {request.delta:+d} unidades. Versión esperada: {request.expected_version}."
        if success:
            detalle += f" Nuevo stock: {prod.stock}, nueva versión: {prod.version}."
            # Publicar evento en tiempo real
            evento = inventory_pb2.ProductEvent(
                tipo=inventory_pb2.EventType.UPDATED,
                product=_producto_a_proto(prod),
                origen_sede=emisor["sede_nombre"],
                timestamp=int(time.time()),
            )
            self.bus_eventos.publicar(evento)

        self._registrar_auditoria(
            emisor=emisor,
            operacion="AJUSTAR_STOCK",
            producto_id=request.product_id,
            detalle=detalle,
            exito=success,
            mensaje=error_msg,
        )

        response_kwargs = {
            "success": success,
            "error_message": error_msg,
            "version_conflict": conflict,
        }
        if prod is not None:
            response_kwargs["product"] = _producto_a_proto(prod)

        return inventory_pb2.AdjustStockResponse(**response_kwargs)

    def SetStock(self, request, context):
        """Establece una cantidad exacta de existencias con control de versión."""
        emisor = self._obtener_emisor(context)
        success, error_msg, conflict, prod = self.almacen.set_stock(
            product_id=request.product_id,
            new_stock=request.new_stock,
            expected_version=request.expected_version,
        )

        detalle = f"Fijar stock a {request.new_stock} unidades. Versión esperada: {request.expected_version}."
        if success:
            detalle += f" Nuevo stock: {prod.stock}, nueva versión: {prod.version}."
            evento = inventory_pb2.ProductEvent(
                tipo=inventory_pb2.EventType.UPDATED,
                product=_producto_a_proto(prod),
                origen_sede=emisor["sede_nombre"],
                timestamp=int(time.time()),
            )
            self.bus_eventos.publicar(evento)

        self._registrar_auditoria(
            emisor=emisor,
            operacion="FIJAR_STOCK",
            producto_id=request.product_id,
            detalle=detalle,
            exito=success,
            mensaje=error_msg,
        )

        response_kwargs = {
            "success": success,
            "error_message": error_msg,
            "version_conflict": conflict,
        }
        if prod is not None:
            response_kwargs["product"] = _producto_a_proto(prod)

        return inventory_pb2.SetStockResponse(**response_kwargs)

    # ==========================================================================
    # 4. GESTIÓN CRUD DE PRODUCTOS
    # ==========================================================================

    def CreateProduct(self, request, context):
        """Registra un nuevo producto en el catálogo central."""
        emisor = self._obtener_emisor(context)
        success, error_msg, prod = self.almacen.create_product(
            product_id=request.product_id,
            name=request.name,
            stock=request.stock,
        )

        detalle = f"Crear producto '{request.name}' (ID: {request.product_id}, Stock inicial: {request.stock})."
        if success:
            evento = inventory_pb2.ProductEvent(
                tipo=inventory_pb2.EventType.CREATED,
                product=_producto_a_proto(prod),
                origen_sede=emisor["sede_nombre"],
                timestamp=int(time.time()),
            )
            self.bus_eventos.publicar(evento)

        self._registrar_auditoria(
            emisor=emisor,
            operacion="CREAR_PRODUCTO",
            producto_id=request.product_id,
            detalle=detalle,
            exito=success,
            mensaje=error_msg,
        )

        response_kwargs = {
            "success": success,
            "error_message": error_msg,
            "version_conflict": False,
        }
        if prod is not None:
            response_kwargs["product"] = _producto_a_proto(prod)

        return inventory_pb2.CreateProductResponse(**response_kwargs)

    def UpdateProduct(self, request, context):
        """Actualiza el nombre de un producto con control de versión."""
        emisor = self._obtener_emisor(context)
        success, error_msg, conflict, prod = self.almacen.update_product(
            product_id=request.product_id,
            name=request.name,
            expected_version=request.expected_version,
        )

        detalle = f"Actualizar nombre a '{request.name}'. Versión esperada: {request.expected_version}."
        if success:
            evento = inventory_pb2.ProductEvent(
                tipo=inventory_pb2.EventType.UPDATED,
                product=_producto_a_proto(prod),
                origen_sede=emisor["sede_nombre"],
                timestamp=int(time.time()),
            )
            self.bus_eventos.publicar(evento)

        self._registrar_auditoria(
            emisor=emisor,
            operacion="EDITAR_PRODUCTO",
            producto_id=request.product_id,
            detalle=detalle,
            exito=success,
            mensaje=error_msg,
        )

        response_kwargs = {
            "success": success,
            "error_message": error_msg,
            "version_conflict": conflict,
        }
        if prod is not None:
            response_kwargs["product"] = _producto_a_proto(prod)

        return inventory_pb2.UpdateProductResponse(**response_kwargs)

    def DeleteProduct(self, request, context):
        """Elimina un producto del catálogo verificando expected_version."""
        emisor = self._obtener_emisor(context)
        prod_previo = self.almacen.get_product(request.product_id)

        success, error_msg, conflict = self.almacen.delete_product(
            product_id=request.product_id,
            expected_version=request.expected_version,
        )

        detalle = f"Eliminar producto. Versión esperada: {request.expected_version}."
        if success and prod_previo is not None:
            evento = inventory_pb2.ProductEvent(
                tipo=inventory_pb2.EventType.DELETED,
                product=_producto_a_proto(prod_previo),
                origen_sede=emisor["sede_nombre"],
                timestamp=int(time.time()),
            )
            self.bus_eventos.publicar(evento)

        self._registrar_auditoria(
            emisor=emisor,
            operacion="ELIMINAR_PRODUCTO",
            producto_id=request.product_id,
            detalle=detalle,
            exito=success,
            mensaje=error_msg,
        )

        response_kwargs = {
            "success": success,
            "error_message": error_msg,
            "version_conflict": conflict,
        }
        if prod_previo is not None:
            response_kwargs["product"] = _producto_a_proto(prod_previo)

        return inventory_pb2.DeleteProductResponse(**response_kwargs)

    # ==========================================================================
    # 5. STREAMING EN TIEMPO REAL
    # ==========================================================================

    def WatchProducts(self, request, context):
        """
        Transmite en streaming los eventos en vivo a clientes suscritos.
        Limpia recursos al desconectarse el cliente sin dejar colas ni hilos colgados.
        """
        emisor = self._obtener_emisor(context)
        cola_cliente = self.bus_eventos.suscribir()
        cliente_id = request.client_id or emisor["username"]
        logger.info(f"Iniciando stream de eventos para '{cliente_id}' ({emisor['sede_nombre']}).")

        def _al_desconectar():
            logger.info(f"Cliente '{cliente_id}' desconectado. Liberando cola de eventos.")
            self.bus_eventos.desuscribir(cola_cliente)

        context.add_callback(_al_desconectar)

        try:
            while context.is_active():
                try:
                    evento = cola_cliente.get(timeout=1.0)
                    yield evento
                except Exception:
                    # Timeout regular para comprobar context.is_active() periódicamente
                    continue
        finally:
            self.bus_eventos.desuscribir(cola_cliente)

    # ==========================================================================
    # 6. HISTORIAL DE AUDITORÍA
    # ==========================================================================

    def ListMovements(self, request, context):
        """Consulta el historial inmutable de movimientos y auditoría."""
        limite = request.limite if request.limite > 0 else 50
        producto_id = request.product_id.strip() if request.product_id else None

        movs = []
        if hasattr(self.almacen, "listar_movimientos"):
            movs = self.almacen.listar_movimientos(producto_id=producto_id, limit=limite)

        movs_proto = []
        for m in movs:
            movs_proto.append(
                inventory_pb2.Movement(
                    id=m["id"],
                    fecha=str(m["fecha"]),
                    usuario=m["usuario"] or "",
                    sede=m["sede"] or "",
                    operacion=m["operacion"] or "",
                    producto_id=m["producto_id"] or "",
                    detalle=m["detalle"] or "",
                    exito=bool(m["exito"]),
                    mensaje=m["mensaje"] or "",
                )
            )

        return inventory_pb2.ListMovementsResponse(
            success=True,
            error_message="",
            movements=movs_proto,
        )
