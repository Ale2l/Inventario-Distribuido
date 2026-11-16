"""
Pruebas exhaustivas sin GUI para ClienteInventario (src/client/cliente_inventario.py).

Verifica la conexión, autenticación, traducción de excepciones a tipos de dominio
en español, control de acceso RBAC por rol, streaming en tiempo real y tolerancia a
fallos (caída y recuperación del servidor gRPC).
"""

import time
import unittest
import queue

from src.server.servidor import crear_servidor
from src.server.interceptor import GestorSesiones
from src.server.bus_eventos import BusEventos
from src.storage.inventario_memoria import InventarioMemoria
from src.client.cliente_inventario import (
    ClienteInventario,
    ServidorNoDisponible,
    TiempoAgotado,
    NoAutenticado,
    PermisoDenegado,
)
from src.generated import inventory_pb2


class TestClienteInventario(unittest.TestCase):
    """Suite de pruebas para ClienteInventario contra servidor gRPC real en puerto dinámico."""

    @classmethod
    def setUpClass(cls):
        """Inicia servidor gRPC central con interceptor de seguridad activo en puerto 0."""
        cls.almacen = InventarioMemoria()
        cls.sesiones = GestorSesiones()
        cls.bus_eventos = BusEventos()
        cls.server = crear_servidor(
            port=0,
            almacen=cls.almacen,
            gestor_sesiones=cls.sesiones,
            bus_eventos=cls.bus_eventos,
            usar_interceptor=True,
        )
        cls.server.start()
        cls.port = cls.server.bound_port
        cls.host = "127.0.0.1"

    @classmethod
    def tearDownClass(cls):
        """Detiene el servidor central."""
        cls.server.stop(grace=0)

    def test_01_conectar_exitoso_y_propiedades_rol_sede(self):
        """1. Conexión exitosa autentica y expone rol, sede y token."""
        cliente = ClienteInventario(timeout=2.0)
        try:
            exito = cliente.conectar(self.host, self.port, "sede_sur", "Sur#2026")
            self.assertTrue(exito)
            self.assertTrue(cliente.esta_autenticado)
            self.assertEqual(cliente.rol, "SEDE")
            self.assertEqual(cliente.sede, "Sede Sur")
            self.assertEqual(cliente.usuario, "sede_sur")
            self.assertTrue(len(cliente.token) > 20)
        finally:
            cliente.cerrar()

    def test_02_conectar_credenciales_invalidas(self):
        """2. Credenciales incorrectas retornan False sin lanzar excepción no controlada."""
        cliente = ClienteInventario(timeout=2.0)
        try:
            exito = cliente.conectar(self.host, self.port, "sede_sur", "ClaveErronea")
            self.assertFalse(exito)
            self.assertFalse(cliente.esta_autenticado)
            self.assertIsNone(cliente.token)
        finally:
            cliente.cerrar()

    def test_03_conectar_servidor_caido_lanza_servidornodisponible(self):
        """3. Conectar a un host/puerto inactivo traduce a ServidorNoDisponible."""
        cliente = ClienteInventario(timeout=0.5)
        try:
            with self.assertRaises(ServidorNoDisponible):
                cliente.conectar("127.0.0.1", 59998, "sede_sur", "Sur#2026")
        finally:
            cliente.cerrar()

    def test_04_llamadas_rpc_negocio_con_token(self):
        """4. Invocar operaciones de inventario adjunta automáticamente el token en metadata."""
        cliente = ClienteInventario(timeout=3.0)
        try:
            cliente.conectar(self.host, self.port, "sede_sur", "Sur#2026")

            # Listar
            resp_list = cliente.listar_productos()
            self.assertTrue(resp_list.success)
            self.assertTrue(len(resp_list.products) >= 5)

            # Consultar
            resp_get = cliente.consultar_producto("PROD-001")
            self.assertTrue(resp_get.success)
            self.assertEqual(resp_get.product.name, "Laptop ThinkPad T14")
            ver_actual = resp_get.product.version

            # Ajustar stock
            resp_adj = cliente.ajustar_stock("PROD-001", delta=3, expected_version=ver_actual)
            self.assertTrue(resp_adj.success)
            self.assertEqual(resp_adj.product.version, ver_actual + 1)

            # Establecer stock
            resp_set = cliente.establecer_stock("PROD-001", new_stock=45, expected_version=ver_actual + 1)
            self.assertTrue(resp_set.success)
            self.assertEqual(resp_set.product.stock, 45)

            # Crear producto
            resp_crear = cliente.crear_producto("P-TEST-CLIENT", "Impresora Epson", 8)
            self.assertTrue(resp_crear.success)
            self.assertEqual(resp_crear.product.id, "P-TEST-CLIENT")

            # Editar producto
            resp_edit = cliente.actualizar_producto("P-TEST-CLIENT", "Impresora Epson EcoTank", 1)
            self.assertTrue(resp_edit.success)
            self.assertEqual(resp_edit.product.name, "Impresora Epson EcoTank")

            # Eliminar producto
            resp_del = cliente.eliminar_producto("P-TEST-CLIENT", expected_version=2)
            self.assertTrue(resp_del.success)

            # Historial de auditoría
            resp_movs = cliente.listar_movimientos(limite=5)
            self.assertTrue(resp_movs.success)
            self.assertTrue(len(resp_movs.movements) > 0)

        finally:
            cliente.cerrar()

    def test_05_traduccion_permiso_denegado(self):
        """5. Invocación de método no autorizado para el rol traduce a PermisoDenegado."""
        cliente = ClienteInventario(timeout=3.0)
        try:
            # Login con simulador (no tiene permisos para crear productos)
            cliente.conectar(self.host, self.port, "simulador", "Sim#2026")
            self.assertEqual(cliente.rol, "SIMULADOR")

            with self.assertRaises(PermisoDenegado):
                cliente.crear_producto("P-FORBIDDEN", "Item Prohibido", 5)

            with self.assertRaises(PermisoDenegado):
                cliente.eliminar_producto("PROD-001", expected_version=1)

        finally:
            cliente.cerrar()

    def test_06_suscripcion_en_vivo_y_recepcion_eventos(self):
        """6. Suscribir recibe eventos en tiempo real mediante hilo daemon."""
        cliente_sub = ClienteInventario(timeout=3.0)
        cliente_pub = ClienteInventario(timeout=3.0)

        cola_eventos = queue.Queue()
        estados_recibidos = []

        def on_evento(ev):
            cola_eventos.put(ev)

        def on_estado(estado, detalle):
            estados_recibidos.append((estado, detalle))

        try:
            cliente_sub.conectar(self.host, self.port, "sede_sur", "Sur#2026")
            cliente_pub.conectar(self.host, self.port, "sede_norte", "Norte#2026")

            # Iniciar suscripción
            cliente_sub.suscribir(callback_evento=on_evento, callback_estado=on_estado)
            time.sleep(0.3)

            # Publicar una modificación desde Sede Norte
            p_actual = cliente_pub.consultar_producto("PROD-002").product
            resp_adj = cliente_pub.ajustar_stock("PROD-002", delta=2, expected_version=p_actual.version)
            self.assertTrue(resp_adj.success)

            # Verificar que el suscriptor reciba el ProductEvent en tiempo real
            evento = cola_eventos.get(timeout=3.0)
            self.assertEqual(evento.tipo, inventory_pb2.EventType.UPDATED)
            self.assertEqual(evento.product.id, "PROD-002")
            self.assertEqual(evento.origen_sede, "Sede Norte")

        finally:
            cliente_sub.desuscribir()
            cliente_sub.cerrar()
            cliente_pub.cerrar()

    def test_07_tolerancia_a_fallos_servidor_caido_y_reconexion(self):
        """
        7. Detiene el servidor, verifica que el cliente reporta ServidorNoDisponible,
        y comprueba que se reconecta automáticamente cuando el servidor vuelve a iniciar.
        """
        sesiones_temp = GestorSesiones()
        almacen_temp = InventarioMemoria()
        bus_temp = BusEventos()

        # Iniciar servidor temporal en puerto dinámico
        server_temp = crear_servidor(
            port=0,
            almacen=almacen_temp,
            gestor_sesiones=sesiones_temp,
            bus_eventos=bus_temp,
            usar_interceptor=True,
        )
        server_temp.start()
        port_temp = server_temp.bound_port

        cliente = ClienteInventario(timeout=0.5)
        estados_stream = []
        eventos_reconexion = queue.Queue()

        def on_estado(st, msg):
            estados_stream.append((st, msg))

        def on_evento(ev):
            eventos_reconexion.put(ev)

        try:
            # 1. Conectar y suscribir
            cliente.conectar("127.0.0.1", port_temp, "sede_sur", "Sur#2026")
            cliente.suscribir(callback_evento=on_evento, callback_estado=on_estado)
            time.sleep(0.3)

            # 2. Detener servidor temporal abruptamente
            server_temp.stop(grace=0)
            time.sleep(0.5)

            # 3. Llamada unaria reporta ServidorNoDisponible
            with self.assertRaises(ServidorNoDisponible):
                cliente.consultar_producto("PROD-001")

            # 4. Verificar que el stream detectó la caída y notificó 'reconectando'
            time.sleep(1.0)
            self.assertTrue(
                any(st == "reconectando" for st, _ in estados_stream),
                "El cliente debió notificar estado 'reconectando' al caer el servidor.",
            )

            # 5. Reiniciar el servidor en el mismo puerto con las mismas sesiones
            server_restaurado = crear_servidor(
                port=port_temp,
                almacen=almacen_temp,
                gestor_sesiones=sesiones_temp,
                bus_eventos=bus_temp,
                usar_interceptor=True,
            )
            server_restaurado.start()

            try:
                # 6. Esperar a que el cliente se reconecte automáticamente
                time.sleep(2.0)
                self.assertTrue(
                    any(st == "conectado" for st, _ in estados_stream[-3:]),
                    "El cliente debió restablecer el estado 'conectado' al revivir el servidor.",
                )

                # 7. Publicar un cambio y verificar que el cliente reconectado lo recibe
                p_cur = almacen_temp.get_product("PROD-001")
                bus_temp.publicar(
                    inventory_pb2.ProductEvent(
                        tipo=inventory_pb2.EventType.UPDATED,
                        product=inventory_pb2.Product(id="PROD-001", name=p_cur.name, stock=p_cur.stock + 1, version=p_cur.version + 1),
                        origen_sede="Servidor Reanudado",
                        timestamp=int(time.time()),
                    )
                )

                evento_recibido = eventos_reconexion.get(timeout=3.0)
                self.assertEqual(evento_recibido.product.id, "PROD-001")
                self.assertEqual(evento_recibido.origen_sede, "Servidor Reanudado")

            finally:
                server_restaurado.stop(grace=0)

        finally:
            cliente.desuscribir()
            cliente.cerrar()


if __name__ == "__main__":
    unittest.main()
