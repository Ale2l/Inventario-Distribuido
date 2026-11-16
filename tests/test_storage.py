"""
Pruebas para la capa de persistencia en PostgreSQL (InventarioPostgres).

Verifica operaciones CRUD, control de concurrencia optimista atómica en SQL,
persistencia tras recrear el pool de conexiones, auditoría de movimientos y
autenticación segura con bcrypt sobre la base de datos inventario_test.
"""

import threading
import unittest
from typing import List, Dict, Any
import psycopg

from src.storage.inventario_postgres import InventarioPostgres
from src.storage.inventario_memoria import InventarioMemoria, Producto
from src.config import (
    DB_HOST,
    DB_PORT,
    DB_TEST_NAME,
    DB_USER,
    DB_PASSWORD,
    USUARIOS_INICIALES,
)


def _es_base_datos_test_accesible() -> bool:
    """Verifica si la base de datos inventario_test está accesible con las credenciales de inventario_app."""
    try:
        conn = psycopg.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_TEST_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            connect_timeout=1,
            autocommit=True,
        )
        conn.close()
        return True
    except Exception:
        return False


BASE_TEST_DISPONIBLE = _es_base_datos_test_accesible()


@unittest.skipUnless(
    BASE_TEST_DISPONIBLE,
    f"Base de datos '{DB_TEST_NAME}' no inicializada aún en PostgreSQL. "
    "Ejecute 'python scripts/init_db.py' con la contraseña de postgres.",
)
class TestInventarioPostgres(unittest.TestCase):
    """Suite de pruebas de persistencia, CRUD y concurrencia sobre PostgreSQL inventario_test."""

    @classmethod
    def setUpClass(cls):
        """Inicializa el almacén conectado a la base de datos de pruebas."""
        cls.almacen = InventarioPostgres(dbname=DB_TEST_NAME)

    @classmethod
    def tearDownClass(cls):
        """Cierra el pool de conexiones al finalizar la suite."""
        cls.almacen.close()

    def setUp(self):
        """
        Limpia las tablas y restablece los 5 productos de prueba en cada test
        para garantizar aislamiento e independencia total entre pruebas.
        """
        with self.almacen.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE movimientos, productos RESTART IDENTITY;")
                # Insertar los 5 productos base del catálogo
                cur.execute(
                    """
                    INSERT INTO productos (id, nombre, stock, version) VALUES
                    ('PROD-001', 'Laptop ThinkPad T14', 15, 1),
                    ('PROD-002', 'Mouse Inalámbrico Logitech', 50, 1),
                    ('PROD-003', 'Teclado Mecánico Keychron K2', 30, 1),
                    ('PROD-004', 'Monitor Dell 27 Pulgadas 4K', 20, 1),
                    ('PROD-005', 'Auriculares Sony WH-1000XM5', 10, 1);
                    """
                )

    def test_inicializacion_catalogo(self):
        """1. Verificar que el almacén contenga los 5 productos iniciales en versión 1."""
        productos = self.almacen.list_products()
        self.assertEqual(len(productos), 5)
        ids = {p.id for p in productos}
        self.assertIn("PROD-001", ids)
        self.assertIn("PROD-002", ids)
        self.assertIn("PROD-003", ids)
        self.assertIn("PROD-004", ids)
        self.assertIn("PROD-005", ids)
        for p in productos:
            self.assertEqual(p.version, 1)

    def test_consultar_producto_existente(self):
        """2. Consultar un producto existente retorna los datos esperados de PostgreSQL."""
        prod = self.almacen.get_product("PROD-001")
        self.assertIsNotNone(prod)
        self.assertEqual(prod.id, "PROD-001")
        self.assertEqual(prod.name, "Laptop ThinkPad T14")
        self.assertEqual(prod.stock, 15)
        self.assertEqual(prod.version, 1)

    def test_consultar_producto_inexistente(self):
        """3. Consultar un producto inexistente retorna None."""
        prod = self.almacen.get_product("PROD-INEXISTENTE")
        self.assertIsNone(prod)

    def test_consultar_producto_ids_invalidos(self):
        """4. Validar que identificadores vacíos o nulos retornen None sin excepción."""
        self.assertIsNone(self.almacen.get_product(""))
        self.assertIsNone(self.almacen.get_product("   "))
        self.assertIsNone(self.almacen.get_product(None))

    def test_listar_productos(self):
        """5. Listar productos retorna todos los elementos ordenados por ID."""
        productos = self.almacen.list_products()
        self.assertEqual(len(productos), 5)
        self.assertEqual(productos[0].id, "PROD-001")
        self.assertEqual(productos[4].id, "PROD-005")

    def test_ajustar_stock_incremento_valido(self):
        """6. Aumentar stock de forma válida incrementa existencias y versión atómicamente."""
        success, err, conflict, p = self.almacen.adjust_stock(
            product_id="PROD-001", delta=5, expected_version=1
        )
        self.assertTrue(success)
        self.assertFalse(conflict)
        self.assertEqual(err, "")
        self.assertEqual(p.stock, 20)
        self.assertEqual(p.version, 2)

        # Confirmar en base de datos
        prod_db = self.almacen.get_product("PROD-001")
        self.assertEqual(prod_db.stock, 20)
        self.assertEqual(prod_db.version, 2)

    def test_ajustar_stock_decremento_valido(self):
        """7. Disminuir stock de forma válida resta existencias e incrementa versión."""
        success, err, conflict, p = self.almacen.adjust_stock(
            product_id="PROD-001", delta=-5, expected_version=1
        )
        self.assertTrue(success)
        self.assertFalse(conflict)
        self.assertEqual(p.stock, 10)
        self.assertEqual(p.version, 2)

    def test_ajustar_stock_rechazo_stock_negativo(self):
        """8. Rechazar ajustes que dejen el stock en valores negativos."""
        success, err, conflict, p = self.almacen.adjust_stock(
            product_id="PROD-001", delta=-20, expected_version=1
        )
        self.assertFalse(success)
        self.assertFalse(conflict)
        self.assertIn("insuficiente", err.lower())

        # Verificar que el registro en BD permanezca intacto
        prod_db = self.almacen.get_product("PROD-001")
        self.assertEqual(prod_db.stock, 15)
        self.assertEqual(prod_db.version, 1)

    def test_ajustar_stock_rechazo_version_desactualizada(self):
        """9. Rechazar ajustes cuando expected_version no coincide (conflicto optimista)."""
        success, err, conflict, p = self.almacen.adjust_stock(
            product_id="PROD-001", delta=5, expected_version=99
        )
        self.assertFalse(success)
        self.assertTrue(conflict)
        self.assertIn("conflicto de versión", err.lower())

        prod_db = self.almacen.get_product("PROD-001")
        self.assertEqual(prod_db.stock, 15)
        self.assertEqual(prod_db.version, 1)

    def test_ajustar_stock_producto_inexistente(self):
        """10. Rechazar ajuste sobre un producto que no existe en BD."""
        success, err, conflict, p = self.almacen.adjust_stock(
            product_id="PROD-INEXISTENTE", delta=5, expected_version=1
        )
        self.assertFalse(success)
        self.assertFalse(conflict)
        self.assertIn("no encontrado", err.lower())
        self.assertIsNone(p)

    def test_establecer_stock_valido(self):
        """11. Establecer una cantidad específica de existencias válida."""
        success, err, conflict, p = self.almacen.set_stock(
            product_id="PROD-002", new_stock=75, expected_version=1
        )
        self.assertTrue(success)
        self.assertFalse(conflict)
        self.assertEqual(p.stock, 75)
        self.assertEqual(p.version, 2)

        prod_db = self.almacen.get_product("PROD-002")
        self.assertEqual(prod_db.stock, 75)
        self.assertEqual(prod_db.version, 2)

    def test_establecer_stock_rechazo_negativo(self):
        """12. Rechazar establecimiento de stock negativo."""
        success, err, conflict, p = self.almacen.set_stock(
            product_id="PROD-002", new_stock=-10, expected_version=1
        )
        self.assertFalse(success)
        self.assertFalse(conflict)
        self.assertIn("negativo", err.lower())

        prod_db = self.almacen.get_product("PROD-002")
        self.assertEqual(prod_db.stock, 50)
        self.assertEqual(prod_db.version, 1)

    def test_establecer_stock_rechazo_version_desactualizada(self):
        """13. Rechazar set_stock con versión desactualizada."""
        success, err, conflict, p = self.almacen.set_stock(
            product_id="PROD-002", new_stock=100, expected_version=5
        )
        self.assertFalse(success)
        self.assertTrue(conflict)

        prod_db = self.almacen.get_product("PROD-002")
        self.assertEqual(prod_db.stock, 50)
        self.assertEqual(prod_db.version, 1)

    def test_operaciones_rechazadas_no_alteran_datos(self):
        """14. Múltiples operaciones inválidas consecutivas preservan el estado original."""
        self.almacen.adjust_stock("PROD-003", delta=-100, expected_version=1)
        self.almacen.adjust_stock("PROD-003", delta=10, expected_version=2)
        self.almacen.set_stock("PROD-003", new_stock=-5, expected_version=1)
        self.almacen.set_stock("PROD-003", new_stock=40, expected_version=99)

        prod_final = self.almacen.get_product("PROD-003")
        self.assertEqual(prod_final.stock, 30)
        self.assertEqual(prod_final.version, 1)

    # ==========================================================================
    # NUEVAS PRUEBAS REQUERIDAS EN TAREA 2: CRUD, PERSISTENCIA Y CONCURRENCIA
    # ==========================================================================

    def test_crud_crear_producto_exitoso(self):
        """15. Crear un nuevo producto en PostgreSQL."""
        success, err, prod = self.almacen.create_product("PROD-NEW", "Impresora Láser HP", 8)
        self.assertTrue(success)
        self.assertEqual(err, "")
        self.assertEqual(prod.id, "PROD-NEW")
        self.assertEqual(prod.name, "Impresora Láser HP")
        self.assertEqual(prod.stock, 8)
        self.assertEqual(prod.version, 1)

        db_check = self.almacen.get_product("PROD-NEW")
        self.assertIsNotNone(db_check)
        self.assertEqual(db_check.name, "Impresora Láser HP")

    def test_crud_crear_producto_duplicado_rechazado(self):
        """16. Rechazar la creación de un producto con ID duplicado."""
        success, err, prod = self.almacen.create_product("PROD-001", "Laptop Duplicada", 5)
        self.assertFalse(success)
        self.assertIn("ya existe", err.lower())
        self.assertIsNone(prod)

    def test_crud_crear_producto_stock_negativo(self):
        """17. Rechazar la creación de un producto con stock negativo."""
        success, err, prod = self.almacen.create_product("PROD-NEG", "Producto Inválido", -3)
        self.assertFalse(success)
        self.assertIn("negativo", err.lower())
        self.assertIsNone(prod)

    def test_crud_editar_nombre_exitoso(self):
        """18. Actualizar el nombre de un producto con versión correcta."""
        success, err, conflict, prod = self.almacen.update_product(
            "PROD-001", name="Laptop ThinkPad T14 Gen 4", expected_version=1
        )
        self.assertTrue(success)
        self.assertFalse(conflict)
        self.assertEqual(prod.name, "Laptop ThinkPad T14 Gen 4")
        self.assertEqual(prod.version, 2)

        prod_db = self.almacen.get_product("PROD-001")
        self.assertEqual(prod_db.name, "Laptop ThinkPad T14 Gen 4")
        self.assertEqual(prod_db.version, 2)

    def test_crud_editar_nombre_conflicto_version(self):
        """19. Rechazar la edición de nombre si la versión está desactualizada."""
        success, err, conflict, prod = self.almacen.update_product(
            "PROD-001", name="Nombre Inválido", expected_version=99
        )
        self.assertFalse(success)
        self.assertTrue(conflict)
        self.assertIn("conflicto de versión", err.lower())

    def test_crud_eliminar_producto_exitoso(self):
        """20. Eliminar un producto con versión esperada válida."""
        success, err, conflict = self.almacen.delete_product("PROD-005", expected_version=1)
        self.assertTrue(success)
        self.assertFalse(conflict)
        self.assertIsNone(self.almacen.get_product("PROD-005"))

    def test_crud_eliminar_producto_conflicto_version(self):
        """21. Rechazar la eliminación de un producto con versión desactualizada."""
        success, err, conflict = self.almacen.delete_product("PROD-005", expected_version=50)
        self.assertFalse(success)
        self.assertTrue(conflict)
        self.assertIsNotNone(self.almacen.get_product("PROD-005"))

    def test_persistencia_tras_recrear_pool(self):
        """
        22. Demostrar persistencia real en PostgreSQL tras cerrar y recrear el pool.
        """
        # Modificar stock en el almacén actual
        self.almacen.adjust_stock("PROD-001", delta=10, expected_version=1)

        # Crear una segunda instancia completamente nueva con su propio pool independiente
        nuevo_almacen = InventarioPostgres(dbname=DB_TEST_NAME)
        try:
            prod_persistido = nuevo_almacen.get_product("PROD-001")
            self.assertEqual(prod_persistido.stock, 25)
            self.assertEqual(prod_persistido.version, 2)
        finally:
            nuevo_almacen.close()

    def test_concurrencia_hilos_atomic_sql(self):
        """
        23. Concurrencia real con hilos contra PostgreSQL:
        Múltiples hilos intentan modificar el mismo producto con la misma versión esperada.
        PostgreSQL debe aplicar el bloqueo a nivel de fila y permitir exactamente 1 éxito.
        """
        product_id = "PROD-004"
        version_inicial = 1
        num_hilos = 6
        delta = 2

        barrera = threading.Barrier(num_hilos)
        resultados: List[Dict[str, Any]] = []
        lock_res = threading.Lock()

        def worker(idx):
            barrera.wait(timeout=5.0)
            success, err, conflict, prod = self.almacen.adjust_stock(
                product_id=product_id, delta=delta, expected_version=version_inicial
            )
            with lock_res:
                resultados.append({
                    "hilo": idx,
                    "success": success,
                    "conflict": conflict,
                    "error": err,
                })

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(num_hilos)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        exitos = [r for r in resultados if r["success"]]
        conflictos = [r for r in resultados if r["conflict"]]

        # Exactamente 1 actualización atómica aceptada por PostgreSQL
        self.assertEqual(len(exitos), 1, "Solo 1 transacción debió actualizar la fila.")
        self.assertEqual(
            len(conflictos),
            num_hilos - 1,
            f"Las otras {num_hilos - 1} transacciones debieron recibir conflicto.",
        )

        prod_final = self.almacen.get_product(product_id)
        self.assertEqual(prod_final.stock, 20 + delta)
        self.assertEqual(prod_final.version, 2)

    def test_movimientos_auditoria(self):
        """24. Registrar y consultar el historial inmutable de movimientos."""
        self.almacen.registrar_movimiento(
            usuario="sede_sur",
            sede="Sede Sur",
            operacion="AJUSTE_STOCK",
            producto_id="PROD-001",
            detalle="Incremento de 5 unidades",
            exito=True,
            mensaje="Operación procesada",
        )

        movs = self.almacen.listar_movimientos(producto_id="PROD-001", limit=10)
        self.assertTrue(len(movs) >= 1)
        ultimo = movs[0]
        self.assertEqual(ultimo["usuario"], "sede_sur")
        self.assertEqual(ultimo["operacion"], "AJUSTE_STOCK")
        self.assertTrue(ultimo["exito"])

    def test_autenticacion_usuario_bcrypt(self):
        """25. Autenticar usuarios contra la tabla de PostgreSQL con hash bcrypt."""
        # Usuario y clave válidos
        user_info = self.almacen.autenticar_usuario("sede_sur", "Sur#2026")
        self.assertIsNotNone(user_info)
        self.assertEqual(user_info["username"], "sede_sur")
        self.assertEqual(user_info["rol"], "SEDE")
        self.assertEqual(user_info["sede_nombre"], "Sede Sur")

        # Contraseña incorrecta
        self.assertIsNone(self.almacen.autenticar_usuario("sede_sur", "ClaveErronea"))

        # Usuario inexistente
        self.assertIsNone(self.almacen.autenticar_usuario("usuario_inexistente", "Sur#2026"))


class TestInventarioMemoriaContrato(unittest.TestCase):
    """Mantiene las pruebas unitarias del almacén en memoria para garantizar retrocompatibilidad."""

    def setUp(self):
        self.almacen = InventarioMemoria()

    def test_memoria_inicializacion(self):
        self.assertEqual(len(self.almacen.list_products()), 5)

    def test_memoria_ajuste_stock(self):
        success, _, _, p = self.almacen.adjust_stock("PROD-001", delta=5, expected_version=1)
        self.assertTrue(success)
        self.assertEqual(p.stock, 20)
        self.assertEqual(p.version, 2)

    def test_memoria_conflicto_version(self):
        success, _, conflict, _ = self.almacen.adjust_stock("PROD-001", delta=5, expected_version=99)
        self.assertFalse(success)
        self.assertTrue(conflict)


if __name__ == "__main__":
    unittest.main()
