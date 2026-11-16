"""
Pruebas unitarias para la interfaz gráfica (CustomTkinter) del cliente.

Verifica la inicialización de los componentes visuales, el flujo de autenticación,
la actualización reactiva ante eventos gRPC en tiempo real, el manejo de
conflictos de versión y la respuesta visual ante desconexiones del servidor.
"""

import unittest
from unittest.mock import MagicMock, patch
import time

import customtkinter as ctk

from src.client.cliente_gui import (
    AppInventario,
    FrameLogin,
    FrameSede,
    FrameSimuladorPlaceholder,
)
from src.client.cliente_inventario import ClienteInventario
from src.config import GRPC_DEFAULT_CLIENT_HOST, GRPC_SERVER_PORT
from src.generated import inventory_pb2


class TestClienteGUI(unittest.TestCase):
    """Pruebas funcionales de los componentes de la interfaz CustomTkinter."""

    def setUp(self):
        # Crear la app principal ocultando la ventana para no interrumpir el escritorio
        self.app = AppInventario()
        self.app.withdraw()

    def tearDown(self):
        try:
            self.app.destroy()
        except Exception:
            pass

    def test_01_inicializacion_y_pantalla_login(self):
        """1. AppInventario inicia mostrando el FrameLogin con los campos por defecto."""
        self.assertIsInstance(self.app.frame_actual, FrameLogin)
        frame_login = self.app.frame_actual

        # Comprobar valores por defecto
        self.assertEqual(frame_login.entry_ip.get(), GRPC_DEFAULT_CLIENT_HOST)
        self.assertEqual(frame_login.entry_puerto.get(), str(GRPC_SERVER_PORT))
        self.assertEqual(frame_login.entry_user.get(), "sede_sur")
        self.assertEqual(frame_login.entry_pass.get(), "Sur#2026")

    def test_02_validacion_campos_login_vacios(self):
        """2. Validar que campos vacíos en el login impiden la conexión y muestran error."""
        frame_login = self.app.frame_actual
        frame_login.entry_user.delete(0, "end")

        frame_login._iniciar_login()
        self.assertIn("complete todos los campos", frame_login.lbl_error.cget("text"))

        # Validar puerto no numérico
        frame_login.entry_user.insert(0, "sede_sur")
        frame_login.entry_puerto.delete(0, "end")
        frame_login.entry_puerto.insert(0, "abc")
        frame_login._iniciar_login()
        self.assertIn("número entero", frame_login.lbl_error.cget("text"))

    def test_03_transicion_de_pantallas_rol_sede(self):
        """3. Redirige a FrameSede cuando el rol autenticado es SEDE."""
        self.app.cliente._rol = "SEDE"
        self.app.cliente._sede = "Sede Sur"
        self.app.cliente._usuario = "sede_sur"

        frame_login = self.app.frame_actual
        frame_login._procesar_resultado_login(True, None)

        self.assertIsInstance(self.app.frame_actual, FrameSede)
        self.assertIn("Sede Sur", self.app.title())

    def test_04_transicion_de_pantallas_rol_simulador(self):
        """4. Redirige a FrameSimuladorPlaceholder cuando el rol autenticado es SIMULADOR."""
        self.app.cliente._rol = "SIMULADOR"
        self.app.cliente._sede = "Simulador"
        self.app.cliente._usuario = "simulador"

        frame_login = self.app.frame_actual
        frame_login._procesar_resultado_login(True, None)

        self.assertTrue(
            isinstance(self.app.frame_actual, FrameSimuladorPlaceholder)
            or "Simulador" in type(self.app.frame_actual).__name__
        )
        self.assertIn("Simulador", self.app.title())

    def test_05_catalogo_e_interfaz_frame_sede(self):
        """5. FrameSede inicializa correctamente la tabla Treeview y los botones de acción."""
        self.app.cliente._rol = "SEDE"
        self.app.cliente._sede = "Sede Norte"
        self.app.cliente._usuario = "sede_norte"
        self.app.cliente._host = "127.0.0.1"
        self.app.cliente._puerto = 50051

        # Mockear métodos del cliente con respuestas gRPC tipadas
        productos_lista = [
            inventory_pb2.Product(id="PROD-001", name="Laptop Gamer Pro", stock=15, version=1),
            inventory_pb2.Product(id="PROD-002", name="Monitor 4K UHD", stock=8, version=2),
        ]
        self.app.cliente.listar_productos = MagicMock(
            return_value=inventory_pb2.ListProductsResponse(success=True, products=productos_lista)
        )
        self.app.cliente.listar_movimientos = MagicMock(
            return_value=inventory_pb2.ListMovementsResponse(success=True, movements=[])
        )
        self.app.cliente.suscribir = MagicMock()

        self.app.mostrar_panel_sede()
        frame_sede: FrameSede = self.app.frame_actual

        # Esperar/procesar el renderizado en la tabla
        frame_sede._renderizar_inventario(productos_lista)

        self.assertIsInstance(frame_sede, FrameSede)
        self.assertEqual(len(frame_sede.tree_inv.get_children()), 2)
        self.assertIn("PROD-001", frame_sede._productos_cache)

        # Probar filtrado por texto
        frame_sede.entry_filtro.insert(0, "Laptop")
        frame_sede._aplicar_filtro_tabla()
        items_filtrados = frame_sede.tree_inv.get_children()
        self.assertEqual(len(items_filtrados), 1)

    def test_06_procesamiento_evento_en_tiempo_real(self):
        """6. Al llegar un ProductEvent remoto, FrameSede actualiza la tabla y muestra notificación."""
        self.app.cliente._rol = "SEDE"
        self.app.cliente._sede = "Sede Sur"
        productos_lista = [
            inventory_pb2.Product(id="PROD-001", name="Laptop Gamer Pro", stock=15, version=1),
        ]
        self.app.cliente.listar_productos = MagicMock(
            return_value=inventory_pb2.ListProductsResponse(success=True, products=productos_lista)
        )
        self.app.cliente.listar_movimientos = MagicMock(
            return_value=inventory_pb2.ListMovementsResponse(success=True, movements=[])
        )
        self.app.cliente.suscribir = MagicMock()

        self.app.mostrar_panel_sede()
        frame_sede: FrameSede = self.app.frame_actual
        frame_sede._renderizar_inventario(productos_lista)

        # Simular evento de actualización proveniente de "Sede Norte"
        evento = inventory_pb2.ProductEvent(
            tipo=inventory_pb2.EventType.UPDATED,
            product=inventory_pb2.Product(id="PROD-001", name="Laptop Gamer Pro", stock=20, version=2),
            origen_sede="Sede Norte",
            timestamp=int(time.time()),
        )

        frame_sede._procesar_evento_remoto(evento)

        # Verificar que el producto en caché se actualizó a versión 2 y stock 20
        self.assertEqual(frame_sede._productos_cache["PROD-001"].stock, 20)
        self.assertEqual(frame_sede._productos_cache["PROD-001"].version, 2)

        # Verificar mensaje del banner de notificación
        texto_banner = frame_sede.banner_evento.cget("text")
        self.assertIn("Sede Norte", texto_banner)
        self.assertIn("PROD-001", texto_banner)

    def test_07_tolerancia_a_desconexion_servidor(self):
        """7. Notificación visual y bloqueo de botones cuando el servidor se desconecta."""
        self.app.cliente._rol = "SEDE"
        self.app.cliente._sede = "Sede Sur"
        self.app.cliente.listar_productos = MagicMock(
            return_value=inventory_pb2.ListProductsResponse(success=True, products=[])
        )
        self.app.cliente.listar_movimientos = MagicMock(
            return_value=inventory_pb2.ListMovementsResponse(success=True, movements=[])
        )
        self.app.cliente.suscribir = MagicMock()

        self.app.mostrar_panel_sede()
        frame_sede: FrameSede = self.app.frame_actual

        # Notificar reconexión/desconexión
        frame_sede._actualizar_estado_red("reconectando", "Conexión perdida con el servidor central")

        # Verificar estado visual de alerta
        self.assertFalse(frame_sede._servidor_activo)
        self.assertIn("Reconectando", frame_sede.lbl_estado_conexion.cget("text"))
        self.assertEqual(frame_sede.btn_crear.cget("state"), "disabled")
        self.assertEqual(frame_sede.btn_ajustar.cget("state"), "disabled")

        # Notificar recuperación
        frame_sede._actualizar_estado_red("conectado", "Conectado al servidor central")
        self.assertTrue(frame_sede._servidor_activo)
        self.assertIn("Conectado", frame_sede.lbl_estado_conexion.cget("text"))
        self.assertEqual(frame_sede.btn_crear.cget("state"), "normal")


if __name__ == "__main__":
    unittest.main()
