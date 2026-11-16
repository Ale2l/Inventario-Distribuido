"""
Pruebas de verificación para el contrato gRPC extendido (protos/inventory.proto).
Comprueba la existencia y tipado de los nuevos métodos RPC, enums y mensajes protobuf.
"""

import unittest
import grpc
from src.generated import inventory_pb2, inventory_pb2_grpc


class TestContratoProtoExtendido(unittest.TestCase):
    """Verifica la integridad de los mensajes y stubs generados."""

    def test_metodos_en_servicer_y_stub(self):
        """Verifica que InventoryServiceStub y InventoryServiceServicer contengan los 10 RPCs."""
        metodos_esperados = [
            # Base
            "GetProduct",
            "ListProducts",
            "AdjustStock",
            "SetStock",
            # Nuevos requeridos en Tarea 3
            "Login",
            "CreateProduct",
            "UpdateProduct",
            "DeleteProduct",
            "WatchProducts",
            "ListMovements",
        ]

        # Comprobar en la clase Servicer
        for m in metodos_esperados:
            self.assertTrue(
                hasattr(inventory_pb2_grpc.InventoryServiceServicer, m),
                f"El servicer gRPC debe definir el método {m}",
            )

        # Comprobar en una instancia del Stub (en gRPC Python, los métodos se ligan en __init__)
        canal_dummy = grpc.insecure_channel("localhost:50051")
        try:
            stub = inventory_pb2_grpc.InventoryServiceStub(canal_dummy)
            for m in metodos_esperados:
                self.assertTrue(
                    hasattr(stub, m),
                    f"El stub gRPC debe implementar el método {m}",
                )
        finally:
            canal_dummy.close()

    def test_mensajes_login(self):
        """Verifica la estructura de LoginRequest y LoginResponse."""
        req = inventory_pb2.LoginRequest(username="sede_sur", password="password")
        self.assertEqual(req.username, "sede_sur")
        self.assertEqual(req.password, "password")

        resp = inventory_pb2.LoginResponse(
            success=True,
            error_message="",
            token="jwt-token-123",
            rol="SEDE",
            sede_nombre="Sede Sur",
        )
        self.assertTrue(resp.success)
        self.assertEqual(resp.token, "jwt-token-123")
        self.assertEqual(resp.rol, "SEDE")
        self.assertEqual(resp.sede_nombre, "Sede Sur")

    def test_mensajes_crud(self):
        """Verifica mensajes de CreateProduct, UpdateProduct y DeleteProduct."""
        # Create
        req_c = inventory_pb2.CreateProductRequest(product_id="P-01", name="Monitor", stock=10)
        self.assertEqual(req_c.product_id, "P-01")
        resp_c = inventory_pb2.CreateProductResponse(
            success=True,
            product=inventory_pb2.Product(id="P-01", name="Monitor", stock=10, version=1),
        )
        self.assertTrue(resp_c.success)
        self.assertEqual(resp_c.product.version, 1)

        # Update
        req_u = inventory_pb2.UpdateProductRequest(product_id="P-01", name="Monitor OLED", expected_version=1)
        self.assertEqual(req_u.name, "Monitor OLED")
        self.assertEqual(req_u.expected_version, 1)

        # Delete
        req_d = inventory_pb2.DeleteProductRequest(product_id="P-01", expected_version=2)
        self.assertEqual(req_d.product_id, "P-01")
        self.assertEqual(req_d.expected_version, 2)

    def test_mensajes_watch_streaming_y_eventos(self):
        """Verifica el enum EventType, WatchRequest y ProductEvent."""
        self.assertEqual(inventory_pb2.EventType.CREATED, 0)
        self.assertEqual(inventory_pb2.EventType.UPDATED, 1)
        self.assertEqual(inventory_pb2.EventType.DELETED, 2)

        req_w = inventory_pb2.WatchRequest(client_id="sede_sur_gui")
        self.assertEqual(req_w.client_id, "sede_sur_gui")

        evento = inventory_pb2.ProductEvent(
            tipo=inventory_pb2.EventType.UPDATED,
            product=inventory_pb2.Product(id="PROD-001", name="Laptop", stock=18, version=2),
            origen_sede="Sede Norte",
            timestamp=1700000000,
        )
        self.assertEqual(evento.tipo, inventory_pb2.EventType.UPDATED)
        self.assertEqual(evento.origen_sede, "Sede Norte")
        self.assertEqual(evento.product.stock, 18)

    def test_mensajes_historial_movimientos(self):
        """Verifica ListMovementsRequest, Movement y ListMovementsResponse."""
        req_m = inventory_pb2.ListMovementsRequest(limite=50, product_id="PROD-001")
        self.assertEqual(req_m.limite, 50)
        self.assertEqual(req_m.product_id, "PROD-001")

        mov = inventory_pb2.Movement(
            id=1,
            fecha="2026-10-30T14:00:00Z",
            usuario="sede_norte",
            sede="Sede Norte",
            operacion="AJUSTAR_STOCK",
            producto_id="PROD-001",
            detalle="Incremento de 3 unidades",
            exito=True,
            mensaje="Operación satisfactoria",
        )
        resp_m = inventory_pb2.ListMovementsResponse(
            success=True,
            movements=[mov],
        )
        self.assertTrue(resp_m.success)
        self.assertEqual(len(resp_m.movements), 1)
        self.assertEqual(resp_m.movements[0].usuario, "sede_norte")


if __name__ == "__main__":
    unittest.main()
