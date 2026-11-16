"""
Pruebas unitarias para el Simulador Gráfico de Concurrencia (CustomTkinter).

Verifica la parametrización de hilos y delta, la carga del catálogo, los modos
de concurrencia ("Sin reintento" y "Con reintento"), el cálculo automático de
la invariante matemática de consistencia y la exportación de evidencias a archivo .txt.
"""

import glob
import os
import unittest
from unittest.mock import MagicMock, patch
import time

import customtkinter as ctk

from src.client.cliente_gui import AppInventario
from src.client.cliente_inventario import ClienteInventario
from src.client.simulador_gui import FrameSimulador
from src.generated import inventory_pb2


class TestSimuladorGUI(unittest.TestCase):
    """Suite de pruebas para la interfaz y lógica del FrameSimulador."""

    def setUp(self):
        self.app = AppInventario()
        self.app.withdraw()

        self.cliente = ClienteInventario(timeout=2.0)
        self.cliente._token = "fake-simulador-token"
        self.cliente._rol = "SIMULADOR"
        self.cliente._sede = "Simulador"
        self.cliente._usuario = "simulador"
        self.cliente._host = "localhost"
        self.cliente._puerto = 50051

        # Instanciar el frame del simulador
        self.frame = FrameSimulador(self.app, self.cliente)
        self.frame.pack(fill="both", expand=True)

    def tearDown(self):
        try:
            self.app.destroy()
        except Exception:
            pass

    def test_01_componentes_y_valores_por_defecto(self):
        """1. Verificar que los campos editables y controles existan con sus valores por defecto."""
        self.assertEqual(self.frame.entry_hilos.get(), "5")
        self.assertEqual(self.frame.entry_delta.get(), "1")
        self.assertEqual(self.frame.var_modo.get(), "sin_reintento")
        self.assertFalse(self.frame._simulacion_en_curso)
        self.assertEqual(self.frame.btn_disparar.cget("state"), "normal")
        self.assertEqual(self.frame.btn_exportar.cget("state"), "disabled")

        # Comprobar columnas del Treeview
        columnas = self.frame.tree_res["columns"]
        self.assertIn("hilo", columnas)
        self.assertIn("resultado", columnas)
        self.assertIn("version_usada", columnas)
        self.assertIn("reintentos", columnas)
        self.assertIn("tiempo", columnas)
        self.assertIn("detalle", columnas)

    def test_02_actualizar_selector_productos(self):
        """2. La respuesta de ListProducts actualiza el OptionMenu de selección."""
        prods = [
            inventory_pb2.Product(id="PROD-001", name="Laptop Gamer", stock=10, version=1),
            inventory_pb2.Product(id="PROD-002", name="Monitor 4K", stock=5, version=3),
        ]
        self.frame._actualizar_selector_productos(prods)

        self.assertEqual(len(self.frame._productos_disponibles), 2)
        opciones = self.frame.cbo_producto.cget("values")
        self.assertEqual(len(opciones), 2)
        self.assertIn("PROD-001", opciones[0])
        self.assertIn("PROD-002", opciones[1])

    def test_03_cambio_de_modo_ejecucion(self):
        """3. Alternar entre modos de ejecución actualiza la variable interna var_modo."""
        self.frame._cambiar_modo("Con reintento (Garantizar todas)")
        self.assertEqual(self.frame.var_modo.get(), "con_reintento")

        self.frame._cambiar_modo("Sin reintento (1 gana, N-1 conflicto)")
        self.assertEqual(self.frame.var_modo.get(), "sin_reintento")

    def test_04_renderizado_e_invariante_satisfecha_sin_reintento(self):
        """4. Validación matemática de la invariante en modo 'Sin reintento' (1 éxito, 4 conflictos)."""
        datos = {
            "timestamp": "2026-10-30 15:00:00",
            "host": "localhost",
            "puerto": 50051,
            "product_id": "PROD-001",
            "nombre_prod": "Laptop Gamer",
            "num_hilos": 5,
            "delta": 2,
            "modo": "sin_reintento",
            "stock_inicial": 10,
            "stock_final": 12,
            "stock_esperado": 12,
            "version_inicial": 1,
            "version_final": 2,
            "version_esperada": 2,
            "exitosas": 1,
            "conflictos": 4,
            "invariante_valida": True,
            "resultados": [
                {"hilo": "Cliente-01", "exito": True, "conflicto": False, "version_usada": 1, "reintentos": 0, "tiempo_ms": 3.5, "detalle": "Aceptado"},
                {"hilo": "Cliente-02", "exito": False, "conflicto": True, "version_usada": 1, "reintentos": 0, "tiempo_ms": 3.7, "detalle": "Conflicto"},
                {"hilo": "Cliente-03", "exito": False, "conflicto": True, "version_usada": 1, "reintentos": 0, "tiempo_ms": 3.8, "detalle": "Conflicto"},
                {"hilo": "Cliente-04", "exito": False, "conflicto": True, "version_usada": 1, "reintentos": 0, "tiempo_ms": 3.9, "detalle": "Conflicto"},
                {"hilo": "Cliente-05", "exito": False, "conflicto": True, "version_usada": 1, "reintentos": 0, "tiempo_ms": 4.1, "detalle": "Conflicto"},
            ],
        }

        self.frame._renderizar_resultados(datos)

        self.assertIn("INVARIANTE SATISFECHA", self.frame.lbl_invariante.cget("text"))
        self.assertEqual(len(self.frame.tree_res.get_children()), 5)
        self.assertEqual(self.frame.btn_exportar.cget("state"), "normal")

    def test_05_renderizado_e_invariante_satisfecha_con_reintento(self):
        """5. Validación matemática de la invariante en modo 'Con reintento' (5 éxitos adaptativos)."""
        datos = {
            "timestamp": "2026-10-30 15:05:00",
            "host": "localhost",
            "puerto": 50051,
            "product_id": "PROD-001",
            "nombre_prod": "Laptop Gamer",
            "num_hilos": 5,
            "delta": 1,
            "modo": "con_reintento",
            "stock_inicial": 20,
            "stock_final": 25,
            "stock_esperado": 25,
            "version_inicial": 1,
            "version_final": 6,
            "version_esperada": 6,
            "exitosas": 5,
            "conflictos": 7,
            "invariante_valida": True,
            "resultados": [
                {"hilo": f"Cliente-{i:02d}", "exito": True, "conflicto": i > 1, "version_usada": i, "reintentos": i - 1, "tiempo_ms": 10.0 + i, "detalle": f"Éxito con {i-1} reintentos"}
                for i in range(1, 6)
            ],
        }

        self.frame._renderizar_resultados(datos)

        self.assertIn("INVARIANTE SATISFECHA", self.frame.lbl_invariante.cget("text"))
        self.assertIn("Stock Final: 25", self.frame.lbl_metric_stock.cget("text"))
        self.assertIn("Versión Final: v6", self.frame.lbl_metric_version.cget("text"))

    def test_06_renderizado_e_invariante_violada(self):
        """6. Detección visual y alerta si la invariante matemática no coincide."""
        datos = {
            "timestamp": "2026-10-30 15:10:00",
            "host": "localhost",
            "puerto": 50051,
            "product_id": "PROD-001",
            "nombre_prod": "Laptop Gamer",
            "num_hilos": 5,
            "delta": 1,
            "modo": "sin_reintento",
            "stock_inicial": 10,
            "stock_final": 15,       # Inconsistente (debería ser 11)
            "stock_esperado": 11,
            "version_inicial": 1,
            "version_final": 1,      # Inconsistente (debería ser 2)
            "version_esperada": 2,
            "exitosas": 1,
            "conflictos": 4,
            "invariante_valida": False,
            "resultados": [],
        }

        self.frame._renderizar_resultados(datos)

        self.assertIn("INVARIANTE VIOLADA", self.frame.lbl_invariante.cget("text"))

    @patch("tkinter.messagebox.showinfo")
    def test_07_exportar_evidencia_archivo_txt(self, mock_msgbox):
        """7. Exportar resultados genera un archivo .txt con fecha/hora y estructura formal."""
        datos = {
            "timestamp": "2026-10-30 15:15:00",
            "host": "localhost",
            "puerto": 50051,
            "product_id": "PROD-001",
            "nombre_prod": "Laptop Gamer",
            "num_hilos": 3,
            "delta": 1,
            "modo": "sin_reintento",
            "stock_inicial": 10,
            "stock_final": 11,
            "stock_esperado": 11,
            "version_inicial": 1,
            "version_final": 2,
            "version_esperada": 2,
            "exitosas": 1,
            "conflictos": 2,
            "invariante_valida": True,
            "resultados": [
                {"hilo": "Cliente-01", "exito": True, "conflicto": False, "version_usada": 1, "reintentos": 0, "tiempo_ms": 4.2, "detalle": "Aceptado"},
                {"hilo": "Cliente-02", "exito": False, "conflicto": True, "version_usada": 1, "reintentos": 0, "tiempo_ms": 4.5, "detalle": "Conflicto"},
                {"hilo": "Cliente-03", "exito": False, "conflicto": True, "version_usada": 1, "reintentos": 0, "tiempo_ms": 4.8, "detalle": "Conflicto"},
            ],
        }

        self.frame._ultimos_resultados = datos
        self.frame._exportar_evidencia()

        # Comprobar que se haya creado un archivo en evidencias/
        archivos = glob.glob(os.path.join("evidencias", "simulacion_*.txt"))
        self.assertTrue(len(archivos) > 0, "Debe existir al menos un archivo de evidencia exportado.")

        # Leer el contenido del archivo más reciente
        archivo_reciente = max(archivos, key=os.path.getmtime)
        with open(archivo_reciente, "r", encoding="utf-8") as f:
            contenido = f.read()

        self.assertIn("EVIDENCIA DE SIMULACIÓN DE CONCURRENCIA DISTRIBUIDA", contenido)
        self.assertIn("PROD-001", contenido)
        self.assertIn("INVARIANTE SATISFECHA", contenido)
        self.assertIn("Cliente-01", contenido)


if __name__ == "__main__":
    unittest.main()

