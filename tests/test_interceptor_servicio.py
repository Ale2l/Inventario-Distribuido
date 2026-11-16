"""
Pruebas unitarias para el servicio gRPC InventoryServiceServicer y AuthInterceptor.

Valida autenticación (Login), generación y verificación de tokens, control de acceso
basado en roles (RBAC: token inválido, rol con permiso, rol sin permiso), auditoría
de movimientos y streaming de eventos en tiempo real.
"""

import time
import unittest
from unittest.mock import MagicMock
import grpc

from src.generated import inventory_pb2, inventory_pb2_grpc
from src.server.servicio import InventoryServiceServicer
from src.server.interceptor import GestorSesiones, AuthInterceptor
from src.server.bus_eventos import BusEventos
from src.server.servidor import crear_servidor
from src.storage.inventario_memoria import InventarioMemoria


class TestServicioYInterceptor(unittest.TestCase):
    """Pruebas del servicio y del interceptor de autenticación/autorización."""

    def setUp(self):
        """Prepara almacén, sesiones, bus y servicer limpios."""
        self.almacen = InventarioMemoria()
        self.sesiones = GestorSesiones()
        self.bus_eventos = BusEventos()
        self.servicer = InventoryServiceServicer(
            almacen=self.almacen,
            gestor_sesiones=self.sesiones,
            bus_eventos=self.bus_eventos,
        )

    # ==========================================================================
    # PRUEBAS DE LOGIN
    # ==========================================================================

    def test_login_exitoso(self):
        """Verifica inicio de sesión válido y generación de token de sesión."""
        req = inventory_pb2.LoginRequest(username="sede_sur", password="Sur#2026")
        context = MagicMock()
        resp = self.servicer.Login(req, context)

        self.assertTrue(resp.success)
        self.assertEqual(resp.error_message, "")
        self.assertTrue(len(resp.token) > 20)
        self.assertEqual(resp.rol, "SEDE")
        self.assertEqual(resp.sede_nombre, "Sede Sur")

        # Verificar que el token existe en el gestor de sesiones
        sesion = self.sesiones.obtener_sesion(resp.token)
        self.assertIsNotNone(sesion)
        self.assertEqual(sesion["username"], "sede_sur")

        # Verificar auditoría
        movs = self.almacen.listar_movimientos()
        self.assertTrue(any(m["operacion"] == "LOGIN" and m["exito"] for m in movs))

    def test_login_fallido_credenciales_invalidas(self):
        """Verifica rechazo de login con contraseña incorrecta y registro en auditoría."""
        req = inventory_pb2.LoginRequest(username="sede_sur", password="ClaveIncorrecta")
        context = MagicMock()
        resp = self.servicer.Login(req, context)

        self.assertFalse(resp.success)
        self.assertIn("incorrectos", resp.error_message.lower())
        self.assertEqual(resp.token, "")

        # Verificar auditoría de intento fallido
        movs = self.almacen.listar_movimientos()
        self.assertTrue(any(m["operacion"] == "LOGIN" and not m["exito"] for m in movs))

    # ==========================================================================
    # PRUEBAS DE INTEGRACIÓN CON INTERCEPTOR SOBRE gRPC REAL
    # ==========================================================================

    def test_interceptor_llamada_sin_token_rechazada(self):
        """Llamada a método protegido sin cabecera authorization es rechazada con UNAUTHENTICATED."""
        server = crear_servidor(
            port=0,
            almacen=self.almacen,
            gestor_sesiones=self.sesiones,
            bus_eventos=self.bus_eventos,
            usar_interceptor=True,
        )
        server.start()
        canal = grpc.insecure_channel(f"127.0.0.1:{server.bound_port}")
        stub = inventory_pb2_grpc.InventoryServiceStub(canal)

        try:
            # Intento de llamar a ListProducts sin token
            with self.assertRaises(grpc.RpcError) as ctx:
                stub.ListProducts(inventory_pb2.ListProductsRequest())
            self.assertEqual(ctx.exception.code(), grpc.StatusCode.UNAUTHENTICATED)
        finally:
            canal.close()
            server.stop(grace=0)

    def test_interceptor_llamada_con_token_invalido_rechazada(self):
        """Llamada con token falso o inexistente es rechazada con UNAUTHENTICATED."""
        server = crear_servidor(
            port=0,
            almacen=self.almacen,
            gestor_sesiones=self.sesiones,
            bus_eventos=self.bus_eventos,
            usar_interceptor=True,
        )
        server.start()
        canal = grpc.insecure_channel(f"127.0.0.1:{server.bound_port}")
        stub = inventory_pb2_grpc.InventoryServiceStub(canal)

        try:
            metadata = (("authorization", "Bearer token_falso_12345"),)
            with self.assertRaises(grpc.RpcError) as ctx:
                stub.ListProducts(inventory_pb2.ListProductsRequest(), metadata=metadata)
            self.assertEqual(ctx.exception.code(), grpc.StatusCode.UNAUTHENTICATED)
        finally:
            canal.close()
            server.stop(grace=0)

    def test_interceptor_rol_con_permiso_permitido(self):
        """Usuario con rol SEDE invoca CreateProduct y es aceptado exitosamente."""
        server = crear_servidor(
            port=0,
            almacen=self.almacen,
            gestor_sesiones=self.sesiones,
            bus_eventos=self.bus_eventos,
            usar_interceptor=True,
        )
        server.start()
        canal = grpc.insecure_channel(f"127.0.0.1:{server.bound_port}")
        stub = inventory_pb2_grpc.InventoryServiceStub(canal)

        try:
            # 1. Login previo para obtener token válido
            resp_login = stub.Login(inventory_pb2.LoginRequest(username="sede_sur", password="Sur#2026"))
            self.assertTrue(resp_login.success)
            token = resp_login.token

            # 2. Invocar CreateProduct con token en metadata
            metadata = (("authorization", f"Bearer {token}"),)
            req_c = inventory_pb2.CreateProductRequest(product_id="PROD-TEST-AUTH", name="Parlante JBL", stock=25)
            resp_c = stub.CreateProduct(req_c, metadata=metadata)
            self.assertTrue(resp_c.success)
            self.assertEqual(resp_c.product.id, "PROD-TEST-AUTH")
        finally:
            canal.close()
            server.stop(grace=0)

    def test_interceptor_rol_sin_permiso_rechazado(self):
        """Usuario con rol SIMULADOR intenta ejecutar CreateProduct y es rechazado con PERMISSION_DENIED."""
        server = crear_servidor(
            port=0,
            almacen=self.almacen,
            gestor_sesiones=self.sesiones,
            bus_eventos=self.bus_eventos,
            usar_interceptor=True,
        )
        server.start()
        canal = grpc.insecure_channel(f"127.0.0.1:{server.bound_port}")
        stub = inventory_pb2_grpc.InventoryServiceStub(canal)

        try:
            # 1. Login como simulador
            resp_login = stub.Login(inventory_pb2.LoginRequest(username="simulador", password="Sim#2026"))
            self.assertTrue(resp_login.success)
            self.assertEqual(resp_login.rol, "SIMULADOR")
            token = resp_login.token

            # 2. Simulador intenta CreateProduct (no autorizado para SIMULADOR)
            metadata = (("authorization", f"Bearer {token}"),)
            req_c = inventory_pb2.CreateProductRequest(product_id="PROD-SIM-FORBIDDEN", name="No Permitido", stock=5)
            with self.assertRaises(grpc.RpcError) as ctx:
                stub.CreateProduct(req_c, metadata=metadata)
            self.assertEqual(ctx.exception.code(), grpc.StatusCode.PERMISSION_DENIED)
            self.assertIn("permiso", ctx.exception.details().lower())

            # 3. Simulador intenta AjustarStock (operación sí permitida para SIMULADOR)
            req_adj = inventory_pb2.AdjustStockRequest(product_id="PROD-001", delta=2, expected_version=1)
            resp_adj = stub.AdjustStock(req_adj, metadata=metadata)
            self.assertTrue(resp_adj.success)
            self.assertEqual(resp_adj.product.stock, 17)
        finally:
            canal.close()
            server.stop(grace=0)

    # ==========================================================================
    # PRUEBAS DE EVENTOS EN VIVO (BUS Y STREAMING)
    # ==========================================================================

    def test_bus_eventos_publicar_y_suscribir(self):
        """Verifica que modificaciones en el servicer publiquen eventos a los suscriptores."""
        q = self.bus_eventos.suscribir()
        try:
            # Ejecutar un ajuste en el servicer
            req_adj = inventory_pb2.AdjustStockRequest(product_id="PROD-001", delta=4, expected_version=1)
            context = MagicMock()
            resp = self.servicer.AdjustStock(req_adj, context)
            self.assertTrue(resp.success)

            # Verificar que la cola del suscriptor reciba el evento inmediatamente
            evento = q.get(timeout=2.0)
            self.assertEqual(evento.tipo, inventory_pb2.EventType.UPDATED)
            self.assertEqual(evento.product.id, "PROD-001")
            self.assertEqual(evento.product.stock, 19)
            self.assertEqual(evento.product.version, 2)
        finally:
            self.bus_eventos.desuscribir(q)


if __name__ == "__main__":
    unittest.main()

