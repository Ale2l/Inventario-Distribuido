"""
Pruebas unitarias para la interfaz gráfica del Servidor Central (CustomTkinter).

Verifica la detección de direcciones IPv4 locales (Wi-Fi, Ethernet, Radmin VPN),
la validación de conectividad con PostgreSQL antes de arrancar, el ciclo de vida
del servidor gRPC en segundo plano, la auditoría de operaciones en tiempo real,
la supervisión de sesiones activas y el cierre ordenado.
"""

import unittest
from unittest.mock import MagicMock, patch
import time

from src.server.servidor_gui import (
    AppServidor,
    obtener_direcciones_red,
)
from src.server.servidor import crear_servidor
from src.server.interceptor import GestorSesiones
from src.storage.inventario_memoria import InventarioMemoria, Producto


class TestServidorGUI(unittest.TestCase):
    """Suite de pruebas para el Centro de Control del Servidor Central."""

    def setUp(self):
        self.app = AppServidor()
        self.app.withdraw()

    def tearDown(self):
        try:
            self.app.al_cerrar_aplicacion()
        except Exception:
            pass

    def test_01_deteccion_direcciones_ipv4(self):
        """1. Detecta direcciones IPv4 locales no loopback y clasifica sus interfaces."""
        direcciones = obtener_direcciones_red(puerto=50051)
        self.assertIsInstance(direcciones, list)

        for d in direcciones:
            self.assertIn("ip", d)
            self.assertIn("puerto", d)
            self.assertIn("tipo", d)
            self.assertFalse(d["ip"].startswith("127."), "No debe incluir IPs loopback")

    def test_02_inicializacion_interfaz_y_componentes(self):
        """2. Componentes principales y pestañas inicializadas con su estado por defecto."""
        self.assertEqual(self.app.btn_iniciar.cget("state"), "normal")
        self.assertEqual(self.app.btn_detener.cget("state"), "disabled")
        self.assertIn("Detenido", self.app.lbl_indicador_estado.cget("text"))

        # Validar tablas
        self.assertIn("id", self.app.tree_inv["columns"])
        self.assertIn("resultado", self.app.tree_log["columns"])
        self.assertIn("usuario", self.app.tree_ses["columns"])

    @patch("tkinter.messagebox.showerror")
    def test_03_fallo_postgresql_bloquea_arranque(self, mock_msgbox):
        """3. Si PostgreSQL no está disponible, muestra mensaje claro y no arranca el servidor."""
        # Invocar directamente el handler de fallo de base de datos
        self.app._mostrar_error_inicio_bd("couldn't get a connection after 3.00 sec")

        self.assertIsNone(self.app._server)
        self.assertEqual(self.app.btn_iniciar.cget("state"), "normal")
        self.assertIn("Error", self.app.lbl_indicador_estado.cget("text"))
        self.assertTrue(mock_msgbox.called)

    def test_04_arranque_y_detencion_servidor(self):
        """4. Servidor arranca en puerto efímero con almacén válido y se detiene ordenadamente."""
        almacen_test = InventarioMemoria()
        self.app._almacen = almacen_test

        # Crear servidor real en puerto efímero (port=0)
        server = crear_servidor(
            port=0,
            almacen=almacen_test,
            gestor_sesiones=self.app._gestor_sesiones,
            bus_eventos=self.app._bus_eventos,
            usar_interceptor=True,
            callback_auditoria=self.app._on_operacion_servidor,
        )
        server.start()
        self.app._server = server

        self.app._notificar_servidor_iniciado()

        self.assertIsNotNone(self.app._server)
        self.assertEqual(self.app.btn_iniciar.cget("state"), "disabled")
        self.assertEqual(self.app.btn_detener.cget("state"), "normal")
        self.assertIn("ACTIVO", self.app.lbl_indicador_estado.cget("text"))

        # Detener
        self.app.detener_servidor(bloqueante=True, grace=0)

        self.assertEqual(self.app.btn_iniciar.cget("state"), "normal")
        self.assertEqual(self.app.btn_detener.cget("state"), "disabled")

    def test_05_auditoria_operaciones_en_vivo_con_colores(self):
        """5. Las operaciones recibidas se insertan en la tabla con colores según resultado."""
        op_exito = {
            "hora": "15:30:00",
            "sede": "Sede Sur",
            "usuario": "sede_sur",
            "operacion": "AdjustStock",
            "producto_id": "PROD-001",
            "exito": True,
            "detalle": "Stock: 15 -> 17 (v2)",
        }
        op_conflicto = {
            "hora": "15:30:01",
            "sede": "Sede Norte",
            "usuario": "sede_norte",
            "operacion": "AdjustStock",
            "producto_id": "PROD-001",
            "exito": False,
            "mensaje": "Conflicto de versión en PROD-001. Esperada: 1, actual: 2",
        }
        op_error = {
            "hora": "15:30:02",
            "sede": "Simulador",
            "usuario": "simulador",
            "operacion": "DeleteProduct",
            "producto_id": "PROD-002",
            "exito": False,
            "mensaje": "Permiso denegado para el rol SIMULADOR",
        }

        self.app._agregar_fila_log(op_exito)
        self.app._agregar_fila_log(op_conflicto)
        self.app._agregar_fila_log(op_error)

        items = self.app.tree_log.get_children()
        self.assertEqual(len(items), 3)

        # Verificar etiquetas de estilo asignadas
        tags_item0 = self.app.tree_log.item(items[0], "tags")
        tags_item1 = self.app.tree_log.item(items[1], "tags")
        tags_item2 = self.app.tree_log.item(items[2], "tags")

        self.assertIn("error", tags_item0)
        self.assertIn("conflicto", tags_item1)
        self.assertIn("exito", tags_item2)

    def test_06_supervision_sesiones_activas(self):
        """6. Las sesiones creadas en el GestorSesiones se reflejan en la tabla de clientes."""
        self.app._gestor_sesiones.crear_sesion({
            "id": 1,
            "username": "sede_sur",
            "rol": "SEDE",
            "sede_nombre": "Sede Sur",
        })
        self.app._gestor_sesiones.crear_sesion({
            "id": 2,
            "username": "simulador",
            "rol": "SIMULADOR",
            "sede_nombre": "Simulador",
        })

        # Forzar ciclo de actualización de sesiones
        self.app._server = MagicMock()
        self.app._actualizacion_periodica()

        items = self.app.tree_ses.get_children()
        self.assertEqual(len(items), 2)
        self.assertIn("2", self.app.lbl_cont_sesiones.cget("text"))

    def test_07_renderizado_catalogo_central(self):
        """7. Renderiza los productos del almacén en la tabla de inventario central."""
        productos = [
            Producto(id="PROD-001", name="Laptop Gamer Pro", stock=10, version=1),
            Producto(id="PROD-002", name="Monitor 4K UHD", stock=5, version=2),
        ]
        self.app._renderizar_tabla_inventario(productos)

        items = self.app.tree_inv.get_children()
        self.assertEqual(len(items), 2)
        self.assertEqual(self.app.tree_inv.item("PROD-001")["values"][1], "Laptop Gamer Pro")


if __name__ == "__main__":
    unittest.main()
