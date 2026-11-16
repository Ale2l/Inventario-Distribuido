"""
Pruebas de concurrencia distribuida para InventarioDistribuido.
Comprueba que múltiples clientes gRPC concurrentes no generen condiciones de carrera ni actualizaciones perdidas.
"""

import threading
import time
import unittest
from typing import List, Dict, Any

from src.server.servidor import crear_servidor
from src.client.cliente_cli import ClienteInventario
from src.client.simulador_concurrente import ejecutar_simulacion, _hilo_cliente
from src.storage.inventario_memoria import InventarioMemoria


class TestConcurrenciaDistribuida(unittest.TestCase):
    """Suite de pruebas de concurrencia y consistencia con servidor gRPC real."""

    @classmethod
    def setUpClass(cls):
        """Inicia el servidor central gRPC real en puerto dinámico."""
        cls.almacen = InventarioMemoria()
        cls.server = crear_servidor(port=0, almacen=cls.almacen)
        cls.server.start()
        cls.port = cls.server.bound_port
        cls.host = "127.0.0.1"
        time.sleep(0.2)

    @classmethod
    def tearDownClass(cls):
        """Detiene de forma limpia el servidor central."""
        cls.server.stop(grace=1)

    def test_concurrencia_disparo_simultaneo_exactamente_un_exito(self):
        """
        1. Verificar que ante N solicitudes simultáneas con la misma versión inicial,
           exactamente una sea aceptada y el resto sean rechazadas por conflicto de versión.
        """
        cliente_admin = ClienteInventario(host=self.host, port=self.port)
        product_id = "PROD-004"

        resp_ini = cliente_admin.consultar_producto(product_id)
        self.assertTrue(resp_ini.success)
        stock_ini = resp_ini.product.stock
        ver_ini = resp_ini.product.version

        num_clientes = 8
        delta = 3
        barrera = threading.Barrier(num_clientes)
        resultados: List[Dict[str, Any]] = []
        lock_res = threading.Lock()
        hilos = []

        for i in range(num_clientes):
            t = threading.Thread(
                target=_hilo_cliente,
                args=(
                    f"TestSede-{i+1}",
                    self.host,
                    self.port,
                    product_id,
                    delta,
                    ver_ini,
                    barrera,
                    resultados,
                    lock_res,
                ),
            )
            hilos.append(t)
            t.start()

        for t in hilos:
            t.join()

        exitos = [r for r in resultados if r["success"]]
        conflictos = [r for r in resultados if r["version_conflict"]]

        # Validación estricta de invariantes optimistas
        self.assertEqual(len(exitos), 1, "Debe haber exactamente 1 transacción aceptada.")
        self.assertEqual(
            len(conflictos),
            num_clientes - 1,
            f"Deben existir exactamente {num_clientes - 1} conflictos de versión.",
        )

        # Consulta final por gRPC
        resp_fin = cliente_admin.consultar_producto(product_id)
        cliente_admin.cerrar()

        self.assertEqual(
            resp_fin.product.stock,
            stock_ini + delta,
            "El stock solo debió incrementarse por la única transacción aceptada.",
        )
        self.assertEqual(
            resp_fin.product.version,
            ver_ini + 1,
            "La versión solo debió incrementarse exactamente una vez.",
        )

    def test_simulador_concurrente_modulo(self):
        """2. Ejecutar la función principal del simulador de concurrencia y verificar su resultado."""
        resultado = ejecutar_simulacion(
            host=self.host,
            port=self.port,
            num_clientes=6,
            product_id="PROD-005",
            delta=2,
        )
        self.assertTrue(resultado, "El simulador de concurrencia debe concluir con éxito y consistencia.")

    def test_concurrencia_patron_reintento_tras_conflicto(self):
        """
        3. Demostrar que un cliente rechazado por conflicto puede reintentar con la nueva versión y tener éxito.
        """
        cliente1 = ClienteInventario(host=self.host, port=self.port, sede="Sede-1")
        cliente2 = ClienteInventario(host=self.host, port=self.port, sede="Sede-2")
        product_id = "PROD-003"

        # Ambas sedes consultan al mismo tiempo y obtienen versión actual
        info1 = cliente1.consultar_producto(product_id)
        info2 = cliente2.consultar_producto(product_id)
        self.assertEqual(info1.product.version, info2.product.version)
        version_compartida = info1.product.version

        # Sede 1 modifica primero con éxito
        resp1 = cliente1.ajustar_stock(product_id, delta=5, expected_version=version_compartida)
        self.assertTrue(resp1.success)

        # Sede 2 intenta modificar con la versión que tenía guardada -> Conflicto
        resp2 = cliente2.ajustar_stock(product_id, delta=2, expected_version=version_compartida)
        self.assertFalse(resp2.success)
        self.assertTrue(resp2.version_conflict)

        # Sede 2 reintenta: consulta la versión actualizada y envía de nuevo la operación
        info2_reintento = cliente2.consultar_producto(product_id)
        self.assertEqual(info2_reintento.product.version, version_compartida + 1)

        resp2_reintento = cliente2.ajustar_stock(
            product_id, delta=2, expected_version=info2_reintento.product.version
        )
        self.assertTrue(resp2_reintento.success)
        self.assertEqual(resp2_reintento.product.version, version_compartida + 2)

        cliente1.cerrar()
        cliente2.cerrar()


if __name__ == "__main__":
    unittest.main()
