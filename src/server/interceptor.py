"""
Interceptor de autenticación y autorización para el servidor gRPC.

Inspecciona cada solicitud entrante, extrae el token portador (Bearer token)
de los metadatos gRPC, valida la existencia de la sesión activa en el
GestorSesiones y verifica que el rol asignado posea permisos suficientes para
el método invocado.
"""

import logging
import secrets
import threading
import time
from typing import Dict, Any, Optional, List
import grpc

from src.roles import (
    Rol,
    tiene_permiso,
    PERMISO_CONSULTAR,
    PERMISO_LISTAR,
    PERMISO_CREAR_PRODUCTO,
    PERMISO_EDITAR_PRODUCTO,
    PERMISO_ELIMINAR_PRODUCTO,
    PERMISO_AJUSTAR_STOCK,
    PERMISO_FIJAR_STOCK,
    PERMISO_VER_HISTORIAL,
    PERMISO_RECIBIR_EVENTOS,
)

logger = logging.getLogger("AuthInterceptor")

# Mapeo de métodos RPC a sus permisos requeridos según el contrato inventory.proto
METODO_A_PERMISO = {
    "GetProduct": PERMISO_CONSULTAR,
    "ListProducts": PERMISO_LISTAR,
    "AdjustStock": PERMISO_AJUSTAR_STOCK,
    "SetStock": PERMISO_FIJAR_STOCK,
    "CreateProduct": PERMISO_CREAR_PRODUCTO,
    "UpdateProduct": PERMISO_EDITAR_PRODUCTO,
    "DeleteProduct": PERMISO_ELIMINAR_PRODUCTO,
    "WatchProducts": PERMISO_RECIBIR_EVENTOS,
    "ListMovements": PERMISO_VER_HISTORIAL,
}


class GestorSesiones:
    """
    Administrador de sesiones en memoria thread-safe.
    Mantiene el mapeo entre tokens criptográficos opacos y la información del usuario autenticado.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._sesiones: Dict[str, Dict[str, Any]] = {}

    def crear_sesion(self, user_info: Dict[str, Any]) -> str:
        """Genera un token seguro y almacena los datos de la sesión."""
        token = secrets.token_urlsafe(32)
        with self._lock:
            self._sesiones[token] = {
                "id": user_info.get("id"),
                "username": user_info["username"],
                "rol": user_info["rol"],
                "sede_nombre": user_info["sede_nombre"],
                "created_at": time.time(),
            }
        logger.info(f"Sesión creada para '{user_info['username']}' ({user_info['sede_nombre']}).")
        return token

    def obtener_sesion(self, token: str) -> Optional[Dict[str, Any]]:
        """Recupera la información de sesión asociada al token proporcionado."""
        if not token:
            return None
        with self._lock:
            sesion = self._sesiones.get(token)
            return dict(sesion) if sesion else None

    def eliminar_sesion(self, token: str) -> bool:
        """Cierra y elimina una sesión activa."""
        with self._lock:
            return self._sesiones.pop(token, None) is not None

    def listar_sesiones(self) -> List[Dict[str, Any]]:
        """Retorna una lista con la información de todas las sesiones activas."""
        with self._lock:
            return [dict(s) for s in self._sesiones.values()]


class AuthInterceptor(grpc.ServerInterceptor):
    """
    Interceptor gRPC que asegura el control de acceso en todas las llamadas del servicio.
    Exenta únicamente el método de inicio de sesión (Login).
    """

    def __init__(self, gestor_sesiones: GestorSesiones, almacen=None):
        self.gestor_sesiones = gestor_sesiones
        self.almacen = almacen

    def intercept_service(self, continuation, handler_call_details):
        """
        Intercepta la ejecución del RPC antes de invocar la lógica de negocio.
        """
        method = handler_call_details.method
        method_name = method.split("/")[-1]

        # Login está exento de autenticación previa
        if method_name == "Login":
            return continuation(handler_call_details)

        handler = continuation(handler_call_details)
        if handler is None:
            return None

        def _validar_acceso(context):
            metadata = dict(context.invocation_metadata())

            # Extraer token de metadata gRPC
            auth_header = ""
            for k, v in metadata.items():
                if k.lower() == "authorization":
                    auth_header = v
                    break

            token = auth_header.strip()
            if token.lower().startswith("bearer "):
                token = token[7:].strip()

            if not token:
                logger.warning(f"[ACCESO DENEGADO] Llamada a '{method_name}' sin token de autorización.")
                context.abort(
                    grpc.StatusCode.UNAUTHENTICATED,
                    "Token de autenticación faltante o formato incorrecto. Debe enviar 'authorization: Bearer <token>'."
                )

            sesion = self.gestor_sesiones.obtener_sesion(token)
            if not sesion:
                logger.warning(f"[ACCESO DENEGADO] Token inválido o expirado en llamada a '{method_name}'.")
                context.abort(
                    grpc.StatusCode.UNAUTHENTICATED,
                    "Token de autenticación inválido o expirado. Inicie sesión nuevamente."
                )

            # Verificar si el rol posee permiso para el método
            permiso_requerido = METODO_A_PERMISO.get(method_name)
            if permiso_requerido and not tiene_permiso(sesion["rol"], permiso_requerido):
                mensaje_denegado = (
                    f"El rol '{sesion['rol']}' no tiene permiso para ejecutar la operación '{method_name}'."
                )
                logger.warning(f"[PERMISO DENEGADO] Usuario '{sesion['username']}' -> {mensaje_denegado}")

                # Registrar auditoría de permiso denegado
                if self.almacen and hasattr(self.almacen, "registrar_movimiento"):
                    try:
                        self.almacen.registrar_movimiento(
                            usuario=sesion["username"],
                            sede=sesion["sede_nombre"],
                            operacion=method_name,
                            producto_id="",
                            detalle="Intento de acceso sin permisos suficientes",
                            exito=False,
                            mensaje=mensaje_denegado,
                        )
                    except Exception as e:
                        logger.error(f"Error al registrar auditoría de permiso denegado: {e}")

                context.abort(grpc.StatusCode.PERMISSION_DENIED, mensaje_denegado)

            # Adjuntar la sesión al contexto para que el Servicer conozca la identidad del emisor
            context.sesion_usuario = sesion
            return sesion

        # Adaptar según el tipo de RPC (Unary o Streaming)
        if handler.unary_unary:
            def wrapped_unary_unary(request, context):
                _validar_acceso(context)
                return handler.unary_unary(request, context)

            return grpc.unary_unary_rpc_method_handler(
                wrapped_unary_unary,
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )

        elif handler.unary_stream:
            def wrapped_unary_stream(request, context):
                _validar_acceso(context)
                return handler.unary_stream(request, context)

            return grpc.unary_stream_rpc_method_handler(
                wrapped_unary_stream,
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )

        return handler

