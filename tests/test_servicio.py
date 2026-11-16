"""
Pruebas unitarias para el servicio gRPC InventoryServiceServicer.
Valida la generación de respuestas protobuf para los cuatro métodos RPC.
"""

import unittest
from src.storage.inventario_memoria import InventarioMemoria
from src.server.servicio import InventoryServiceServicer
from src.generated import inventory_pb2


class TestInventoryServiceServicer(unittest.TestCase):
    """Suite de pruebas unitarias para la capa de servicio gRPC."""

    def setUp(self):
        """Inicializa un almacén limpio y una instancia del servicer para cada prueba."""
        self.almacen = InventarioMemoria()
        self.servicer = InventoryServiceServicer(almacen=self.almacen)

    def test_rpc_get_product_existente(self):
        """1. RPC GetProduct retorna un producto existente con success=True."""
        req = inventory_pb2.GetProductRequest(product_id="PROD-001")
        resp = self.servicer.GetProduct(req, None)

        self.assertTrue(resp.success)
        self.assertEqual(resp.error_message, "")
        self.assertEqual(resp.product.id, "PROD-001")
        self.assertEqual(resp.product.name, "Laptop ThinkPad T14")
        self.assertEqual(resp.product.stock, 15)
        self.assertEqual(resp.product.version, 1)

    def test_rpc_get_product_inexistente(self):
        """2. RPC GetProduct retorna success=False para producto inexistente."""
        req = inventory_pb2.GetProductRequest(product_id="PROD-INEXISTENTE")
        resp = self.servicer.GetProduct(req, None)

        self.assertFalse(resp.success)
        self.assertIn("no encontrado", resp.error_message.lower())
        self.assertFalse(resp.HasField("product"))

    def test_rpc_list_products(self):
        """3. RPC ListProducts retorna todos los productos del inventario."""
        req = inventory_pb2.ListProductsRequest()
        resp = self.servicer.ListProducts(req, None)

        self.assertTrue(resp.success)
        self.assertEqual(resp.error_message, "")
        self.assertEqual(len(resp.products), 5)
        nombres = [p.name for p in resp.products]
        self.assertIn("Laptop ThinkPad T14", nombres)
        self.assertIn("Mouse Inalámbrico Logitech", nombres)

    def test_rpc_adjust_stock_exitoso(self):
        """4. RPC AdjustStock procesa aumentos y decrementos válidos."""
        # Aumentar +10
        req1 = inventory_pb2.AdjustStockRequest(product_id="PROD-001", delta=10, expected_version=1)
        resp1 = self.servicer.AdjustStock(req1, None)

        self.assertTrue(resp1.success)
        self.assertFalse(resp1.version_conflict)
        self.assertEqual(resp1.product.stock, 25)
        self.assertEqual(resp1.product.version, 2)

        # Disminuir -5
        req2 = inventory_pb2.AdjustStockRequest(product_id="PROD-001", delta=-5, expected_version=2)
        resp2 = self.servicer.AdjustStock(req2, None)

        self.assertTrue(resp2.success)
        self.assertEqual(resp2.product.stock, 20)
        self.assertEqual(resp2.product.version, 3)

    def test_rpc_adjust_stock_stock_insuficiente(self):
        """5. RPC AdjustStock rechaza cuando el stock resultante sería negativo."""
        req = inventory_pb2.AdjustStockRequest(product_id="PROD-001", delta=-50, expected_version=1)
        resp = self.servicer.AdjustStock(req, None)

        self.assertFalse(resp.success)
        self.assertFalse(resp.version_conflict)
        self.assertIn("insuficiente", resp.error_message.lower())
        self.assertEqual(resp.product.stock, 15)
        self.assertEqual(resp.product.version, 1)

    def test_rpc_adjust_stock_conflicto_version(self):
        """6. RPC AdjustStock señala version_conflict=True si la versión está desactualizada."""
        req = inventory_pb2.AdjustStockRequest(product_id="PROD-001", delta=5, expected_version=99)
        resp = self.servicer.AdjustStock(req, None)

        self.assertFalse(resp.success)
        self.assertTrue(resp.version_conflict)
        self.assertIn("conflicto de versión", resp.error_message.lower())
        self.assertEqual(resp.product.version, 1)

    def test_rpc_set_stock_exitoso(self):
        """7. RPC SetStock actualiza la cantidad a un valor exacto."""
        req = inventory_pb2.SetStockRequest(product_id="PROD-002", new_stock=100, expected_version=1)
        resp = self.servicer.SetStock(req, None)

        self.assertTrue(resp.success)
        self.assertFalse(resp.version_conflict)
        self.assertEqual(resp.product.stock, 100)
        self.assertEqual(resp.product.version, 2)

    def test_rpc_set_stock_rechazo_negativo(self):
        """8. RPC SetStock rechaza cantidades negativas."""
        req = inventory_pb2.SetStockRequest(product_id="PROD-002", new_stock=-1, expected_version=1)
        resp = self.servicer.SetStock(req, None)

        self.assertFalse(resp.success)
        self.assertFalse(resp.version_conflict)
        self.assertIn("negativo", resp.error_message.lower())

    def test_rpc_set_stock_conflicto_version(self):
        """9. RPC SetStock señala version_conflict=True con versión desactualizada."""
        req = inventory_pb2.SetStockRequest(product_id="PROD-002", new_stock=50, expected_version=42)
        resp = self.servicer.SetStock(req, None)

        self.assertFalse(resp.success)
        self.assertTrue(resp.version_conflict)
        self.assertIn("conflicto de versión", resp.error_message.lower())


if __name__ == "__main__":
    unittest.main()
