"""
Pruebas de integración para InventarioDistribuido.
Valida la comunicación real gRPC entre ClienteInventario y el servidor central.
"""

import time
import unittest
import grpc

from src.server.servidor import crear_servidor
from src.client.cliente_cli import ClienteInventario
from src.storage.inventario_memoria import InventarioMemoria


class TestIntegracionGrpc(unittest.TestCase):
    """Suite de pruebas de integración con servidor gRPC real en puerto dinámico."""

    @classmethod
    def setUpClass(cls):
        """Inicia un servidor gRPC real en un puerto disponible dinámicamente (port=0)."""
        cls.almacen = InventarioMemoria()
        cls.server = crear_servidor(port=0, almacen=cls.almacen)
        cls.server.start()
        cls.port = cls.server.bound_port
        cls.host = "127.0.0.1"
        time.sleep(0.2)

    @classmethod
    def tearDownClass(cls):
        """Detiene de forma limpia el servidor gRPC de prueba."""
        cls.server.stop(grace=1)

    def setUp(self):
        """Crea un cliente nuevo conectado al servidor dinámico."""
        self.cliente = ClienteInventario(
            host=self.host, port=self.port, sede="Sede-Integracion", timeout=3.0
        )

    def tearDown(self):
        """Cierra el canal gRPC del cliente tras cada prueba."""
        self.cliente.cerrar()

    def test_listar_productos_grpc_real(self):
        """1. Llamada gRPC real a ListProducts retorna el catálogo completo."""
        resp = self.cliente.listar_productos()
        self.assertTrue(resp.success)
        self.assertEqual(len(resp.products), 5)

    def test_consultar_producto_existente_grpc_real(self):
        """2. Llamada gRPC real a GetProduct para un producto existente."""
        resp = self.cliente.consultar_producto("PROD-001")
        self.assertTrue(resp.success)
        self.assertEqual(resp.product.id, "PROD-001")
        self.assertEqual(resp.product.name, "Laptop ThinkPad T14")

    def test_consultar_producto_inexistente_grpc_real(self):
        """3. Llamada gRPC real a GetProduct para un producto inexistente."""
        resp = self.cliente.consultar_producto("PROD-NOEXISTE")
        self.assertFalse(resp.success)
        self.assertIn("no encontrado", resp.error_message.lower())

    def test_modificacion_y_verificacion_segunda_consulta_grpc_real(self):
        """4. Modificar stock por gRPC y confirmar mediante una segunda consulta independiente."""
        # 1. Consulta inicial
        resp_ini = self.cliente.consultar_producto("PROD-001")
        stock_ini = resp_ini.product.stock
        ver_ini = resp_ini.product.version

        # 2. Modificación (+6)
        resp_adj = self.cliente.ajustar_stock("PROD-001", delta=6, expected_version=ver_ini)
        self.assertTrue(resp_adj.success)
        self.assertEqual(resp_adj.product.stock, stock_ini + 6)
        self.assertEqual(resp_adj.product.version, ver_ini + 1)

        # 3. Segunda consulta gRPC para verificar persistencia en el servidor
        resp_verif = self.cliente.consultar_producto("PROD-001")
        self.assertTrue(resp_verif.success)
        self.assertEqual(resp_verif.product.stock, stock_ini + 6)
        self.assertEqual(resp_verif.product.version, ver_ini + 1)

    def test_rechazo_stock_insuficiente_grpc_real(self):
        """5. Rechazo gRPC de ajuste que dejaría el stock en negativo."""
        resp_ini = self.cliente.consultar_producto("PROD-002")
        resp_adj = self.cliente.ajustar_stock(
            "PROD-002", delta=-9999, expected_version=resp_ini.product.version
        )
        self.assertFalse(resp_adj.success)
        self.assertFalse(resp_adj.version_conflict)
        self.assertIn("insuficiente", resp_adj.error_message.lower())

        # Segunda consulta para asegurar que los datos no cambiaron
        resp_check = self.cliente.consultar_producto("PROD-002")
        self.assertEqual(resp_check.product.stock, resp_ini.product.stock)
        self.assertEqual(resp_check.product.version, resp_ini.product.version)

    def test_establecer_stock_valido_grpc_real(self):
        """6. Establecer una cantidad exacta de stock mediante gRPC."""
        resp_ini = self.cliente.consultar_producto("PROD-003")
        resp_set = self.cliente.establecer_stock(
            "PROD-003", new_stock=60, expected_version=resp_ini.product.version
        )
        self.assertTrue(resp_set.success)
        self.assertEqual(resp_set.product.stock, 60)
        self.assertEqual(resp_set.product.version, resp_ini.product.version + 1)

    def test_rechazo_conflicto_version_grpc_real(self):
        """7. Rechazo gRPC con bandera version_conflict=True cuando se envía versión vieja."""
        resp_adj = self.cliente.ajustar_stock("PROD-003", delta=5, expected_version=999)
        self.assertFalse(resp_adj.success)
        self.assertTrue(resp_adj.version_conflict)
        self.assertIn("conflicto de versión", resp_adj.error_message.lower())

    def test_error_conexion_servidor_no_disponible(self):
        """8. Comprobar que conectar a un puerto inactivo arroje error gRPC controlado."""
        # Usamos un puerto que no esté en escucha
        cliente_invalido = ClienteInventario(
            host="127.0.0.1", port=59999, timeout=0.5
        )
        try:
            with self.assertRaises(grpc.RpcError) as ctx:
                cliente_invalido.listar_productos()
            codigo = ctx.exception.code()
            self.assertIn(
                codigo,
                [grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED],
            )
        finally:
            cliente_invalido.cerrar()


if __name__ == "__main__":
    unittest.main()
