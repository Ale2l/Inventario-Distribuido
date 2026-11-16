"""
Interfaz gráfica interactiva para el Simulador de Concurrencia Distribuida (CustomTkinter).

Permite configurar y disparar solicitudes concurrentes sincronizadas mediante threading.Barrier,
probar los modos "Sin reintento" (1 éxito, N-1 conflictos) y "Con reintento" (reintento adaptativo),
verificar en vivo las invariantes matemáticas de consistencia y exportar reportes formales a evidencias/.
"""

import datetime
import logging
import os
import random
import threading
import time
from typing import List, Dict, Any, Optional
import tkinter as tk
from tkinter import ttk, messagebox

import customtkinter as ctk
import grpc

from src.client.cliente_inventario import (
    ClienteInventario,
    ServidorNoDisponible,
    TiempoAgotado,
    NoAutenticado,
    PermisoDenegado,
    ErrorInventario,
)
from src.config import (
    GRPC_DEFAULT_CLIENT_HOST,
    GRPC_SERVER_PORT,
    GRPC_TIMEOUT,
    USUARIOS_INICIALES,
)
from src.generated import inventory_pb2, inventory_pb2_grpc

logger = logging.getLogger("SimuladorGUI")


class FrameSimulador(ctk.CTkFrame):
    """
    Panel interactivo del simulador de concurrencia distribuida.
    Permite parametrizar pruebas de estrés, ejecutar hilos sincronizados
    y validar invariantes de consistencia en tiempo real.
    """

    def __init__(self, master, cliente: ClienteInventario):
        super().__init__(master)
        self.master_app = master
        self.cliente = cliente

        self._productos_disponibles: Dict[str, inventory_pb2.Product] = {}
        self._simulacion_en_curso = False
        self._ultimos_resultados: Optional[Dict[str, Any]] = None

        self._crear_interfaz()
        self._cargar_catalogo_productos()

    def _crear_interfaz(self):
        # 1. Barra superior
        barra_sup = ctk.CTkFrame(self, height=60, corner_radius=0)
        barra_sup.pack(fill="x", side="top", padx=0, pady=0)

        lbl_titulo = ctk.CTkLabel(
            barra_sup,
            text="SIMULADOR DE CONCURRENCIA DISTRIBUIDA (gRPC)",
            font=ctk.CTkFont(size=18, weight="bold"),
            text_color="#38bdf8",
        )
        lbl_titulo.pack(side="left", padx=20, pady=15)

        btn_salir = ctk.CTkButton(
            barra_sup,
            text="Cerrar Sesión",
            width=120,
            fg_color="#802b2b",
            hover_color="#a63a3a",
            command=self._cerrar_sesion,
        )
        btn_salir.pack(side="right", padx=20, pady=15)

        # 2. Contenedor de configuración de parámetros
        frame_parametros = ctk.CTkFrame(self, corner_radius=10)
        frame_parametros.pack(fill="x", padx=15, pady=(10, 5))

        # Fila 1: Producto, Hilos, Delta
        frame_fila1 = ctk.CTkFrame(frame_parametros, fg_color="transparent")
        frame_fila1.pack(fill="x", padx=15, pady=(10, 5))

        lbl_prod = ctk.CTkLabel(frame_fila1, text="Producto a probar:", font=ctk.CTkFont(weight="bold"))
        lbl_prod.pack(side="left", padx=(0, 10))

        self.cbo_producto = ctk.CTkOptionMenu(
            frame_fila1,
            width=280,
            values=["Cargando productos..."],
        )
        self.cbo_producto.pack(side="left", padx=(0, 10))

        self.btn_recargar_prod = ctk.CTkButton(
            frame_fila1,
            text="⟳",
            width=36,
            command=self._cargar_catalogo_productos,
        )
        self.btn_recargar_prod.pack(side="left", padx=(0, 25))

        lbl_hilos = ctk.CTkLabel(frame_fila1, text="N° Clientes / Hilos:", font=ctk.CTkFont(weight="bold"))
        lbl_hilos.pack(side="left", padx=(0, 10))

        self.entry_hilos = ctk.CTkEntry(frame_fila1, width=80)
        self.entry_hilos.insert(0, "5")
        self.entry_hilos.pack(side="left", padx=(0, 25))

        lbl_delta = ctk.CTkLabel(frame_fila1, text="Ajuste Delta (+/-):", font=ctk.CTkFont(weight="bold"))
        lbl_delta.pack(side="left", padx=(0, 10))

        self.entry_delta = ctk.CTkEntry(frame_fila1, width=80)
        self.entry_delta.insert(0, "1")
        self.entry_delta.pack(side="left")

        # Fila 2: Selector de Modo y Botón de Disparo
        frame_fila2 = ctk.CTkFrame(frame_parametros, fg_color="transparent")
        frame_fila2.pack(fill="x", padx=15, pady=(5, 10))

        lbl_modo = ctk.CTkLabel(frame_fila2, text="Modo de Ejecución:", font=ctk.CTkFont(weight="bold"))
        lbl_modo.pack(side="left", padx=(0, 10))

        self.var_modo = tk.StringVar(value="sin_reintento")
        self.seg_modo = ctk.CTkSegmentedButton(
            frame_fila2,
            values=["Sin reintento (1 gana, N-1 conflicto)", "Con reintento (Garantizar todas)"],
            command=self._cambiar_modo,
        )
        self.seg_modo.set("Sin reintento (1 gana, N-1 conflicto)")
        self.seg_modo.pack(side="left", padx=(0, 20))

        self.btn_disparar = ctk.CTkButton(
            frame_fila2,
            text="▶ DISPARAR SIMULACIÓN CONCURRENTE",
            font=ctk.CTkFont(size=13, weight="bold"),
            fg_color="#15803d",
            hover_color="#166534",
            height=36,
            command=self._iniciar_simulacion,
        )
        self.btn_disparar.pack(side="left", padx=10, fill="x", expand=True)

        # 3. Panel de Resumen de Métricas e Invariantes
        self.frame_resumen = ctk.CTkFrame(self, corner_radius=10)
        self.frame_resumen.pack(fill="x", padx=15, pady=5)

        # Columnas de resumen
        f_cols = ctk.CTkFrame(self.frame_resumen, fg_color="transparent")
        f_cols.pack(fill="x", padx=15, pady=10)

        # Columna 1: Stock
        self.lbl_metric_stock = ctk.CTkLabel(
            f_cols,
            text="Stock Inicial: --\nStock Final: --",
            font=ctk.CTkFont(size=13),
            justify="left",
        )
        self.lbl_metric_stock.pack(side="left", expand=True, fill="x")

        # Columna 2: Versión
        self.lbl_metric_version = ctk.CTkLabel(
            f_cols,
            text="Versión Inicial: --\nVersión Final: --",
            font=ctk.CTkFont(size=13),
            justify="left",
        )
        self.lbl_metric_version.pack(side="left", expand=True, fill="x")

        # Columna 3: Conteo operaciones
        self.lbl_metric_ops = ctk.CTkLabel(
            f_cols,
            text="Exitosas: --\nConflictos: --",
            font=ctk.CTkFont(size=13),
            justify="left",
        )
        self.lbl_metric_ops.pack(side="left", expand=True, fill="x")

        # Columna 4: Estado de la Invariante
        self.lbl_invariante = ctk.CTkLabel(
            self.frame_resumen,
            text="Invariante de Consistencia: En espera de ejecución",
            font=ctk.CTkFont(size=14, weight="bold"),
            fg_color="#334155",
            corner_radius=6,
            height=34,
        )
        self.lbl_invariante.pack(fill="x", padx=15, pady=(0, 10))

        # 4. Tabla de Resultados Detallados por Hilo
        frame_tabla = ctk.CTkFrame(self)
        frame_tabla.pack(fill="both", expand=True, padx=15, pady=5)

        columnas = ("hilo", "resultado", "version_usada", "reintentos", "tiempo", "detalle")
        self.tree_res = ttk.Treeview(
            frame_tabla,
            columns=columnas,
            show="headings",
            selectmode="browse",
        )

        self.tree_res.heading("hilo", text="Cliente / Hilo")
        self.tree_res.heading("resultado", text="Resultado")
        self.tree_res.heading("version_usada", text="Versión Usada")
        self.tree_res.heading("reintentos", text="Reintentos")
        self.tree_res.heading("tiempo", text="Latencia (ms)")
        self.tree_res.heading("detalle", text="Detalle de Respuesta")

        self.tree_res.column("hilo", width=120, anchor="center")
        self.tree_res.column("resultado", width=160, anchor="center")
        self.tree_res.column("version_usada", width=110, anchor="center")
        self.tree_res.column("reintentos", width=90, anchor="center")
        self.tree_res.column("tiempo", width=110, anchor="center")
        self.tree_res.column("detalle", width=420, anchor="w")

        # Configurar colores de filas
        self.tree_res.tag_configure("exito", foreground="#4ade80")
        self.tree_res.tag_configure("conflicto", foreground="#facc15")
        self.tree_res.tag_configure("error", foreground="#f87171")

        scrollbar_y = ttk.Scrollbar(frame_tabla, orient="vertical", command=self.tree_res.yview)
        self.tree_res.configure(yscrollcommand=scrollbar_y.set)
        scrollbar_y.pack(side="right", fill="y")
        self.tree_res.pack(fill="both", expand=True)

        # 5. Barra Inferior con Botón de Exportar
        barra_inf = ctk.CTkFrame(self, height=45, corner_radius=0)
        barra_inf.pack(fill="x", side="bottom", padx=0, pady=0)

        self.lbl_estado = ctk.CTkLabel(
            barra_inf,
            text="Listo para ejecutar simulación.",
            font=ctk.CTkFont(size=12),
            text_color="gray",
        )
        self.lbl_estado.pack(side="left", padx=15, pady=8)

        self.btn_exportar = ctk.CTkButton(
            barra_inf,
            text="📄 Exportar Evidencia (.txt)",
            font=ctk.CTkFont(size=12, weight="bold"),
            fg_color="#0369a1",
            hover_color="#0284c7",
            state="disabled",
            command=self._exportar_evidencia,
        )
        self.btn_exportar.pack(side="right", padx=15, pady=8)

    def _cambiar_modo(self, valor: str):
        if "Sin reintento" in valor:
            self.var_modo.set("sin_reintento")
        else:
            self.var_modo.set("con_reintento")

    # ==========================================================================
    # CARGA DE PRODUCTOS DESDE EL SERVIDOR (RPC ListProducts)
    # ==========================================================================

    def _cargar_catalogo_productos(self):
        """Carga la lista de productos por gRPC para alimentar el selector."""
        def _tarea():
            try:
                resp = self.cliente.listar_productos()
                try:
                    self.after(0, self._actualizar_selector_productos, resp.products)
                except Exception:
                    pass
            except Exception as e:
                logger.error(f"Error al listar productos: {e}")
                try:
                    self.after(0, self._mostrar_error_carga, str(e))
                except Exception:
                    pass

        threading.Thread(target=_tarea, daemon=True).start()

    def _actualizar_selector_productos(self, productos: List[inventory_pb2.Product]):
        self._productos_disponibles = {p.id: p for p in productos}
        if not productos:
            self.cbo_producto.configure(values=["No hay productos"])
            return

        opciones = [f"{p.id} - {p.name} (Stock: {p.stock}, v{p.version})" for p in productos]
        self.cbo_producto.configure(values=opciones)
        self.cbo_producto.set(opciones[0])
        self.lbl_estado.configure(text=f"Catálogo cargado: {len(productos)} productos disponibles.")

    def _mostrar_error_carga(self, err_msg: str):
        self.lbl_estado.configure(text=f"Error cargando catálogo: {err_msg}")

    # ==========================================================================
    # EJECUCIÓN MULTIHILO CONCURRENTEMENTE SINCRONIZADA
    # ==========================================================================

    def _iniciar_simulacion(self):
        """Valida las entradas e inicia la simulación concurrente en segundo plano."""
        if self._simulacion_en_curso:
            return

        # 1. Obtener producto seleccionado
        prod_sel = self.cbo_producto.get()
        if not prod_sel or "Cargando" in prod_sel or "No hay" in prod_sel:
            messagebox.showwarning("Atención", "Seleccione un producto válido para la prueba.")
            return

        product_id = prod_sel.split(" - ")[0].strip()

        # 2. Validar número de hilos
        try:
            num_hilos = int(self.entry_hilos.get().strip())
            if num_hilos < 1 or num_hilos > 100:
                raise ValueError()
        except ValueError:
            messagebox.showerror("Error", "El número de clientes/hilos debe ser un entero entre 1 y 100.")
            return

        # 3. Validar delta
        try:
            delta = int(self.entry_delta.get().strip())
        except ValueError:
            messagebox.showerror("Error", "El delta de ajuste debe ser un número entero (positivo o negativo).")
            return

        modo = self.var_modo.get()

        # Bloquear controles de la UI
        self._simulacion_en_curso = True
        self.btn_disparar.configure(state="disabled", text="⏳ Simulando solicitudes concurrentes...")
        self.btn_exportar.configure(state="disabled")
        self.lbl_estado.configure(text="Disparando hilos concurrentes sincronizados...")

        # Limpiar tabla
        for item in self.tree_res.get_children():
            self.tree_res.delete(item)

        # Iniciar trabajo en hilo secundario (NUNCA congela la GUI)
        threading.Thread(
            target=self._hilo_simulacion,
            args=(product_id, num_hilos, delta, modo),
            daemon=True,
        ).start()

    def _hilo_simulacion(self, product_id: str, num_hilos: int, delta: int, modo: str):
        """
        Lógica de concurrencia:
        - Consulta el estado inicial por gRPC (GetProduct).
        - Inicializa threading.Barrier(num_hilos).
        - Lanza cada hilo con su propio canal gRPC independiente.
        - Ejecuta AdjustStock (y reintentos si corresponde).
        - Consulta el estado final por gRPC (GetProduct).
        - Evalúa la invariante de consistencia.
        """
        host = self.cliente._host
        puerto = self.cliente._puerto
        token = self.cliente.token
        metadata = (("authorization", f"Bearer {token}"),) if token else ()

        # 1. Consulta inicial
        canal_admin = grpc.insecure_channel(f"{host}:{puerto}")
        stub_admin = inventory_pb2_grpc.InventoryServiceStub(canal_admin)

        try:
            resp_ini = stub_admin.GetProduct(
                inventory_pb2.GetProductRequest(product_id=product_id),
                timeout=5.0,
                metadata=metadata,
            )
        except Exception as e:
            canal_admin.close()
            self.after(0, self._finalizar_con_error, f"Error al consultar producto inicial: {e}")
            return

        if not resp_ini.success:
            canal_admin.close()
            self.after(0, self._finalizar_con_error, f"Producto no encontrado: {resp_ini.error_message}")
            return

        prod_ini = resp_ini.product
        stock_inicial = prod_ini.stock
        version_inicial = prod_ini.version
        nombre_prod = prod_ini.name

        # 2. Configurar barrera y contenedores
        barrera = threading.Barrier(num_hilos)
        resultados: List[Dict[str, Any]] = []
        lock_res = threading.Lock()
        hilos = []

        def _trabajo_cliente(hilo_id: str):
            canal = grpc.insecure_channel(f"{host}:{puerto}")
            stub = inventory_pb2_grpc.InventoryServiceStub(canal)

            try:
                # Sincronización exacta en el punto de disparo
                barrera.wait(timeout=10.0)
            except threading.BrokenBarrierError:
                pass

            t_inicio = time.perf_counter()
            res = {
                "hilo": hilo_id,
                "exito": False,
                "conflicto": False,
                "version_usada": version_inicial,
                "reintentos": 0,
                "tiempo_ms": 0.0,
                "detalle": "",
            }

            try:
                if modo == "sin_reintento":
                    # --- MODO SIN REINTENTO ---
                    # Todos envían la versión inicial compartida
                    req = inventory_pb2.AdjustStockRequest(
                        product_id=product_id,
                        delta=delta,
                        expected_version=version_inicial,
                    )
                    resp = stub.AdjustStock(req, timeout=5.0, metadata=metadata)
                    t_duracion = (time.perf_counter() - t_inicio) * 1000
                    res["tiempo_ms"] = round(t_duracion, 2)
                    res["exito"] = resp.success
                    res["conflicto"] = resp.version_conflict

                    if resp.success:
                        res["detalle"] = f"Aceptado -> Stock: {resp.product.stock}, v{resp.product.version}"
                    elif resp.version_conflict:
                        res["detalle"] = resp.error_message or "Conflicto de versión detectado"
                    else:
                        res["detalle"] = resp.error_message or "Rechazo de negocio"

                else:
                    # --- MODO CON REINTENTO ---
                    # Ante un conflicto, consulta nueva versión y reintenta
                    ver_actual = version_inicial
                    reintentos = 0
                    max_reintentos = 30

                    while reintentos <= max_reintentos:
                        req = inventory_pb2.AdjustStockRequest(
                            product_id=product_id,
                            delta=delta,
                            expected_version=ver_actual,
                        )
                        resp = stub.AdjustStock(req, timeout=5.0, metadata=metadata)

                        if resp.success:
                            res["exito"] = True
                            res["version_usada"] = ver_actual
                            res["reintentos"] = reintentos
                            res["detalle"] = f"Éxito tras {reintentos} reintento(s) -> Stock: {resp.product.stock}, v{resp.product.version}"
                            break
                        elif resp.version_conflict:
                            reintentos += 1
                            res["conflicto"] = True
                            # Consultar versión fresca
                            resp_get = stub.GetProduct(
                                inventory_pb2.GetProductRequest(product_id=product_id),
                                timeout=5.0,
                                metadata=metadata,
                            )
                            if resp_get.success:
                                ver_actual = resp_get.product.version
                            time.sleep(random.uniform(0.002, 0.010))  # Jitter de retroceso
                        else:
                            # Error no recuperable (ej. stock negativo)
                            res["detalle"] = resp.error_message
                            break

                    t_duracion = (time.perf_counter() - t_inicio) * 1000
                    res["tiempo_ms"] = round(t_duracion, 2)
                    if not res["exito"] and not res["detalle"]:
                        res["detalle"] = f"Límite de {max_reintentos} reintentos superado"

            except grpc.RpcError as rpc_err:
                res["detalle"] = f"Error gRPC: {rpc_err.details() or rpc_err.code()}"
            except Exception as ex:
                res["detalle"] = f"Excepción local: {ex}"
            finally:
                canal.close()
                with lock_res:
                    resultados.append(res)

        # 3. Lanzar hilos
        for i in range(1, num_hilos + 1):
            h_nombre = f"Cliente-{i:02d}"
            t = threading.Thread(target=_trabajo_cliente, args=(h_nombre,), daemon=True)
            hilos.append(t)
            t.start()

        for t in hilos:
            t.join()

        # 4. Consulta final por gRPC
        try:
            resp_fin = stub_admin.GetProduct(
                inventory_pb2.GetProductRequest(product_id=product_id),
                timeout=5.0,
                metadata=metadata,
            )
            canal_admin.close()
        except Exception as e:
            canal_admin.close()
            self.after(0, self._finalizar_con_error, f"Error al consultar producto final: {e}")
            return

        prod_fin = resp_fin.product
        stock_final = prod_fin.stock
        version_final = prod_fin.version

        # 5. Cálculos de métricas e invariante
        total_exitosas = sum(1 for r in resultados if r["exito"])
        total_conflictos = sum(1 for r in resultados if r["conflicto"])
        stock_esperado = stock_inicial + (delta * total_exitosas)
        version_esperada = version_inicial + total_exitosas

        invariante_valida = (stock_final == stock_esperado) and (version_final == version_esperada)

        datos = {
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "host": host,
            "puerto": puerto,
            "product_id": product_id,
            "nombre_prod": nombre_prod,
            "num_hilos": num_hilos,
            "delta": delta,
            "modo": modo,
            "stock_inicial": stock_inicial,
            "stock_final": stock_final,
            "stock_esperado": stock_esperado,
            "version_inicial": version_inicial,
            "version_final": version_final,
            "version_esperada": version_esperada,
            "exitosas": total_exitosas,
            "conflictos": total_conflictos,
            "invariante_valida": invariante_valida,
            "resultados": sorted(resultados, key=lambda x: x["hilo"]),
        }

        # Despachar actualización a la interfaz
        self.after(0, self._renderizar_resultados, datos)

    def _renderizar_resultados(self, datos: Dict[str, Any]):
        """Actualiza la tabla y tarjetas de métricas en el hilo principal de Tkinter."""
        self._ultimos_resultados = datos
        self._simulacion_en_curso = False
        self.btn_disparar.configure(state="normal", text="▶ DISPARAR SIMULACIÓN CONCURRENTE")
        self.btn_exportar.configure(state="normal")
        self.lbl_estado.configure(text=f"Simulación finalizada exitosamente a las {datos['timestamp']}.")

        # Renderizar métricas
        self.lbl_metric_stock.configure(
            text=f"Stock Inicial: {datos['stock_inicial']}\nStock Final: {datos['stock_final']}"
        )
        self.lbl_metric_version.configure(
            text=f"Versión Inicial: v{datos['version_inicial']}\nVersión Final: v{datos['version_final']}"
        )
        self.lbl_metric_ops.configure(
            text=f"Exitosas: {datos['exitosas']}\nConflictos: {datos['conflictos']}"
        )

        # Mostrar Invariante
        if datos["invariante_valida"]:
            texto_inv = (
                f"✔ INVARIANTE SATISFECHA (Consistencia Estricta)\n"
                f"Stock: {datos['stock_final']} == {datos['stock_inicial']} + ({datos['delta']} × {datos['exitosas']})  |  "
                f"Versión: {datos['version_final']} == {datos['version_inicial']} + {datos['exitosas']}"
            )
            self.lbl_invariante.configure(text=texto_inv, fg_color="#166534")  # Verde oscuro
        else:
            texto_inv = (
                f"✖ INVARIANTE VIOLADA (Inconsistencia)\n"
                f"Stock obtenido: {datos['stock_final']} (Esperado: {datos['stock_esperado']})  |  "
                f"Versión obtenida: {datos['version_final']} (Esperada: {datos['version_esperada']})"
            )
            self.lbl_invariante.configure(text=texto_inv, fg_color="#991b1b")  # Rojo oscuro

        # Llenar la tabla
        for r in datos["resultados"]:
            if r["exito"]:
                tag = "exito"
                res_txt = "ÉXITO"
            elif r["conflicto"]:
                tag = "conflicto"
                res_txt = "CONFLICTO VERSIÓN"
            else:
                tag = "error"
                res_txt = "ERROR"

            self.tree_res.insert(
                "",
                "end",
                values=(
                    r["hilo"],
                    res_txt,
                    f"v{r['version_usada']}",
                    r["reintentos"],
                    f"{r['tiempo_ms']:.2f}",
                    r["detalle"],
                ),
                tags=(tag,),
            )

        # Refrescar selector de productos con las nuevas existencias
        self._cargar_catalogo_productos()

    def _finalizar_con_error(self, err_msg: str):
        self._simulacion_en_curso = False
        self.btn_disparar.configure(state="normal", text="▶ DISPARAR SIMULACIÓN CONCURRENTE")
        self.lbl_estado.configure(text=f"Error: {err_msg}")
        messagebox.showerror("Error en Simulación", err_msg)

    # ==========================================================================
    # EXPORTACIÓN DE EVIDENCIAS EN FORMATO .TXT
    # ==========================================================================

    def _exportar_evidencia(self):
        """Genera un archivo de reporte formal en la carpeta evidencias/."""
        if not self._ultimos_resultados:
            messagebox.showinfo("Exportar", "No hay resultados de simulación disponibles para exportar.")
            return

        d = self._ultimos_resultados
        timestamp_slug = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        os.makedirs("evidencias", exist_ok=True)
        ruta_archivo = os.path.join("evidencias", f"simulacion_{timestamp_slug}.txt")

        modo_legible = "Sin reintento (1 gana, N-1 conflicto)" if d["modo"] == "sin_reintento" else "Con reintento (Adaptativo)"
        dictamen = "INVARIANTE SATISFECHA - CONSISTENCIA ESTRICTA GARANTIZADA" if d["invariante_valida"] else "INVARIANTE VIOLADA"

        contenido = f"""================================================================================
          EVIDENCIA DE SIMULACIÓN DE CONCURRENCIA DISTRIBUIDA
               SISTEMA DE INVENTARIO DISTRIBUIDO (gRPC)
================================================================================
Fecha y Hora:           {d['timestamp']}
Servidor Central:       {d['host']}:{d['puerto']}
Usuario / Rol:          {self.cliente.usuario or 'simulador'} / {self.cliente.rol or 'SIMULADOR'}
Producto Bajo Prueba:   {d['product_id']} ({d['nombre_prod']})
Número de Hilos:        {d['num_hilos']}
Delta por Solicitud:    {d['delta']:+d}
Modo de Ejecución:      {modo_legible}

--------------------------------------------------------------------------------
                     ESTADO DEL INVENTARIO (PRE Y POST)
--------------------------------------------------------------------------------
Stock Inicial:          {d['stock_inicial']} unidades
Stock Final Obtenido:   {d['stock_final']} unidades (Esperado: {d['stock_esperado']})
Versión Inicial:        v{d['version_inicial']}
Versión Final Obtenida: v{d['version_final']} (Esperada: v{d['version_esperada']})

--------------------------------------------------------------------------------
                     RESUMEN ESTADÍSTICO DE OPERACIONES
--------------------------------------------------------------------------------
Total de Solicitudes Enviadas:       {d['num_hilos']}
Operaciones Exitosas (Aceptadas):   {d['exitosas']}
Rechazadas por Conflicto de Versión: {d['conflictos']}
Invariante Matemática Válida:        {'SÍ' if d['invariante_valida'] else 'NO'}

--------------------------------------------------------------------------------
                     DESGLOSE INDIVIDUAL POR HILO CLIENTE
--------------------------------------------------------------------------------
{"Hilo":<12} | {"Resultado":<20} | {"Versión":<8} | {"Reintentos":<10} | {"Tiempo (ms)":<12} | Detalle
{"-" * 80}
"""
        for r in d["resultados"]:
            res_txt = "ÉXITO" if r["exito"] else ("CONFLICTO VERSIÓN" if r["conflicto"] else "ERROR")
            ver_str = f"v{r['version_usada']}"
            contenido += f"{r['hilo']:<12} | {res_txt:<20} | {ver_str:<8} | {r['reintentos']:<10} | {r['tiempo_ms']:<12.2f} | {r['detalle']}\n"

        contenido += f"""--------------------------------------------------------------------------------
                     EVALUACIÓN FORMAL DE INVARIANTES
--------------------------------------------------------------------------------
Invariante 1 (Conservación de Stock):
  Fórmula:  Stock_Final == Stock_Inicial + (Delta * Exitosas)
  Cálculo:  {d['stock_final']} == {d['stock_inicial']} + ({d['delta']} * {d['exitosas']}) -> {d['stock_final']} == {d['stock_esperado']} [{'CUMPLE' if d['stock_final'] == d['stock_esperado'] else 'FALLA'}]

Invariante 2 (Monotonía de Versión):
  Fórmula:  Versión_Final == Versión_Inicial + Exitosas
  Cálculo:  {d['version_final']} == {d['version_inicial']} + {d['exitosas']} -> {d['version_final']} == {d['version_esperada']} [{'CUMPLE' if d['version_final'] == d['version_esperada'] else 'FALLA'}]

DICTAMEN FINAL: [{dictamen}]
Explicación de Concurrencia:
El servidor central ejecuta la actualización de inventario de forma atómica
dentro de la base de datos PostgreSQL utilizando validación condicional estricta:
UPDATE productos SET stock = stock + delta, version = version + 1
WHERE id = product_id AND version = expected_version AND stock + delta >= 0
Esto garantiza que solo una transacción logre modificar el registro para una
versión dada, rechazando cualquier intento con una versión desactualizada sin
provocar lecturas sucias, actualizaciones perdidas (Lost Updates) ni bloqueos de mesa.
================================================================================
"""

        try:
            with open(ruta_archivo, "w", encoding="utf-8") as f:
                f.write(contenido)
            self.lbl_estado.configure(text=f"Evidencia guardada exitosamente en {ruta_archivo}")
            messagebox.showinfo(
                "Evidencia Exportada",
                f"El reporte de simulación ha sido guardado exitosamente en:\n{ruta_archivo}",
            )
        except Exception as e:
            messagebox.showerror("Error al Exportar", f"No se pudo guardar el archivo de evidencia: {e}")

    def _cerrar_sesion(self):
        if self.cliente:
            try:
                self.cliente.cerrar()
            except Exception:
                pass
        self.master_app.mostrar_login()


# ==============================================================================
# APP STANDALONE PARA EJECUTAR EL SIMULADOR DIRECTAMENTE
# ==============================================================================

class AppSimuladorStandalone(ctk.CTk):
    """Ventana independiente para ejecutar el simulador directamente desde consola."""

    def __init__(self):
        super().__init__()
        self.title("Simulador de Concurrencia Distribuida - InventarioDistribuido")
        self.geometry("1100x750")
        self.minsize(950, 650)

        ctk.set_appearance_mode("Dark")
        ctk.set_default_color_theme("blue")

        self.cliente = ClienteInventario(timeout=GRPC_TIMEOUT)
        self.frame_actual: Optional[ctk.CTkFrame] = None

        # Autoconectar con la cuenta por defecto de simulador
        self._conectar_y_cargar()

        self.protocol("WM_DELETE_WINDOW", self.al_cerrar_aplicacion)

    def _conectar_y_cargar(self):
        """Intenta autenticarse como simulador al iniciar."""
        def _tarea():
            try:
                exito = self.cliente.conectar(
                    GRPC_DEFAULT_CLIENT_HOST,
                    GRPC_SERVER_PORT,
                    "simulador",
                    USUARIOS_INICIALES.get("simulador", {}).get("password", "Sim#2026"),
                )
                if exito:
                    self.after(0, self.mostrar_panel_simulador)
                else:
                    self.after(0, self._mostrar_error_inicio, "Credenciales incorrectas para simulador.")
            except Exception as e:
                self.after(0, self._mostrar_error_inicio, f"No se pudo conectar al servidor: {e}")

        threading.Thread(target=_tarea, daemon=True).start()

    def mostrar_panel_simulador(self):
        if self.frame_actual:
            self.frame_actual.destroy()
        self.frame_actual = FrameSimulador(self, self.cliente)
        self.frame_actual.pack(fill="both", expand=True)

    def mostrar_login(self):
        self.destroy()

    def _mostrar_error_inicio(self, err: str):
        messagebox.showerror(
            "Error de Conexión",
            f"{err}\n\nAsegúrese de que el servidor gRPC central esté en ejecución en {GRPC_DEFAULT_CLIENT_HOST}:{GRPC_SERVER_PORT}.",
        )
        self.destroy()

    def al_cerrar_aplicacion(self):
        if self.cliente:
            try:
                self.cliente.cerrar()
            except Exception:
                pass
        self.destroy()


def main():
    """Punto de arranque standalone: python -m src.client.simulador_gui"""
    app = AppSimuladorStandalone()
    app.mainloop()


if __name__ == "__main__":
    main()
