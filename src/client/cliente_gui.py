"""
Interfaz gráfica interactiva para InventarioDistribuido (CustomTkinter).

Implementa la pantalla de login por credenciales y dirección del servidor,
el panel de control de inventario para el rol SEDE, streaming en tiempo real
con notificación de eventos remotos, gestión de conflictos de versión y
tolerancia a desconexiones de red con reconexión transparente.
"""

import logging
import threading
import time
from typing import Optional, Dict, Any, List
import tkinter as tk
from tkinter import ttk, messagebox

import customtkinter as ctk

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
)
from src.generated import inventory_pb2

logger = logging.getLogger("ClienteGUI")

# Configuración estética de CustomTkinter
ctk.set_appearance_mode("Dark")
ctk.set_default_color_theme("blue")


class AppInventario(ctk.CTk):
    """Ventana principal que gestiona el ciclo de vida de la aplicación."""

    def __init__(self):
        super().__init__()
        self.title("Inventario Distribuido - Sistema Centralizado")
        self.geometry("1100x750")
        self.minsize(950, 650)

        self.cliente = ClienteInventario(timeout=GRPC_TIMEOUT)
        self.frame_actual: Optional[ctk.CTkFrame] = None

        # Mostrar pantalla inicial de inicio de sesión
        self.mostrar_login()

        # Asegurar cierre limpio al cerrar la ventana con la 'X'
        self.protocol("WM_DELETE_WINDOW", self.al_cerrar_aplicacion)

    def cambiar_frame(self, nuevo_frame_cls, *args, **kwargs):
        """Reemplaza el contenido principal de la ventana de forma fluida."""
        if self.frame_actual is not None:
            self.frame_actual.destroy()
        self.frame_actual = nuevo_frame_cls(self, self.cliente, *args, **kwargs)
        self.frame_actual.pack(fill="both", expand=True)

    def mostrar_login(self):
        """Carga la pantalla de autenticación."""
        self.title("Iniciar Sesión - Inventario Distribuido")
        self.cambiar_frame(FrameLogin)

    def mostrar_panel_sede(self):
        """Carga la interfaz completa de inventario para rol SEDE."""
        nombre_sede = self.cliente.sede or "Sede Desconocida"
        self.title(f"Inventario Distribuido - {nombre_sede}")
        self.cambiar_frame(FrameSede)

    def mostrar_panel_simulador(self):
        """Carga el panel interactivo del simulador de concurrencia."""
        from src.client.simulador_gui import FrameSimulador
        nombre_sede = self.cliente.sede or "Simulador"
        self.title(f"Inventario Distribuido - {nombre_sede}")
        self.cambiar_frame(FrameSimulador)

    def al_cerrar_aplicacion(self):
        """Cierra conexiones gRPC y destruye la ventana."""
        if self.cliente:
            try:
                self.cliente.cerrar()
            except Exception:
                pass
        self.destroy()


# ==============================================================================
# 1. PANTALLA DE INICIO DE SESIÓN
# ==============================================================================

class FrameLogin(ctk.CTkFrame):
    """Pantalla de login con captura de IP, puerto y credenciales."""

    def __init__(self, master: AppInventario, cliente: ClienteInventario):
        super().__init__(master)
        self.master_app = master
        self.cliente = cliente

        self._crear_widgets()

    def _crear_widgets(self):
        # Contenedor centrado
        contenedor = ctk.CTkFrame(self, corner_radius=15, width=420)
        contenedor.place(relx=0.5, rely=0.5, anchor="center")

        lbl_titulo = ctk.CTkLabel(
            contenedor,
            text="INVENTARIO DISTRIBUIDO",
            font=ctk.CTkFont(size=22, weight="bold"),
        )
        lbl_titulo.pack(padx=30, pady=(30, 5))

        lbl_subtitulo = ctk.CTkLabel(
            contenedor,
            text="Conexión y Autenticación gRPC",
            font=ctk.CTkFont(size=13),
            text_color="gray",
        )
        lbl_subtitulo.pack(padx=30, pady=(0, 20))

        # Campos de red
        frame_red = ctk.CTkFrame(contenedor, fg_color="transparent")
        frame_red.pack(fill="x", padx=30, pady=5)

        lbl_ip = ctk.CTkLabel(frame_red, text="IP Servidor:")
        lbl_ip.grid(row=0, column=0, sticky="w", padx=(0, 10))
        self.entry_ip = ctk.CTkEntry(frame_red, width=220)
        self.entry_ip.insert(0, GRPC_DEFAULT_CLIENT_HOST)
        self.entry_ip.grid(row=0, column=1, sticky="ew")

        lbl_puerto = ctk.CTkLabel(frame_red, text="Puerto:")
        lbl_puerto.grid(row=1, column=0, sticky="w", padx=(0, 10), pady=10)
        self.entry_puerto = ctk.CTkEntry(frame_red, width=220)
        self.entry_puerto.insert(0, str(GRPC_SERVER_PORT))
        self.entry_puerto.grid(row=1, column=1, sticky="ew", pady=10)

        # Campos de credenciales
        lbl_user = ctk.CTkLabel(contenedor, text="Usuario:")
        lbl_user.pack(anchor="w", padx=30, pady=(10, 2))
        self.entry_user = ctk.CTkEntry(contenedor, width=320, placeholder_text="ej. sede_sur, sede_norte, simulador")
        self.entry_user.pack(padx=30, pady=2)
        self.entry_user.insert(0, "sede_sur")

        lbl_pass = ctk.CTkLabel(contenedor, text="Contraseña:")
        lbl_pass.pack(anchor="w", padx=30, pady=(10, 2))
        self.entry_pass = ctk.CTkEntry(contenedor, width=320, show="*", placeholder_text="Ingrese su contraseña")
        self.entry_pass.pack(padx=30, pady=2)
        self.entry_pass.insert(0, "Sur#2026")

        # Mensaje de error
        self.lbl_error = ctk.CTkLabel(contenedor, text="", text_color="#ff5555", wraplength=340)
        self.lbl_error.pack(padx=30, pady=10)

        # Botón de conexión
        self.btn_login = ctk.CTkButton(
            contenedor,
            text="Conectar al Servidor",
            font=ctk.CTkFont(size=14, weight="bold"),
            height=40,
            command=self._iniciar_login,
        )
        self.btn_login.pack(padx=30, pady=(5, 30), fill="x")

        # Permitir presionar Enter para conectar
        self.entry_pass.bind("<Return>", lambda e: self._iniciar_login())

    def _iniciar_login(self):
        """Ejecuta el login en un hilo secundario para evitar congelar la interfaz."""
        host = self.entry_ip.get().strip()
        puerto_str = self.entry_puerto.get().strip()
        usuario = self.entry_user.get().strip()
        password = self.entry_pass.get()

        if not host or not puerto_str or not usuario or not password:
            self.lbl_error.configure(text="Por favor complete todos los campos.")
            return

        try:
            puerto = int(puerto_str)
        except ValueError:
            self.lbl_error.configure(text="El puerto debe ser un número entero.")
            return

        self.btn_login.configure(state="disabled", text="Conectando...")
        self.lbl_error.configure(text="")

        def _trabajo_login():
            try:
                exito = self.cliente.conectar(host, puerto, usuario, password)
                self.after(0, self._procesar_resultado_login, exito, None)
            except ErrorInventario as err:
                self.after(0, self._procesar_resultado_login, False, str(err))
            except Exception as ex:
                self.after(0, self._procesar_resultado_login, False, f"Error inesperado: {ex}")

        threading.Thread(target=_trabajo_login, daemon=True).start()

    def _procesar_resultado_login(self, exito: bool, mensaje_error: Optional[str]):
        """Procesa el resultado en el hilo principal de Tkinter."""
        self.btn_login.configure(state="normal", text="Conectar al Servidor")

        if mensaje_error:
            self.lbl_error.configure(text=mensaje_error)
            return

        if not exito:
            self.lbl_error.configure(text="Usuario o contraseña incorrectos.")
            return

        # Redirigir según el rol de la cuenta autenticada
        if self.cliente.rol == "SIMULADOR":
            self.master_app.mostrar_panel_simulador()
        else:
            self.master_app.mostrar_panel_sede()


# ==============================================================================
# 2. PANTALLA PRINCIPAL DE SEDE (ROL: SEDE)
# ==============================================================================

class FrameSede(ctk.CTkFrame):
    """Panel de administración de inventario con pestañas de catálogo e historial."""

    def __init__(self, master: AppInventario, cliente: ClienteInventario):
        super().__init__(master)
        self.master_app = master
        self.cliente = cliente

        self._productos_cache: Dict[str, inventory_pb2.Product] = {}
        self._servidor_activo = True

        self._crear_interfaz()
        self._iniciar_streaming()
        self._cargar_inventario()

    def _crear_interfaz(self):
        # 1. Barra superior de título y usuario
        barra_sup = ctk.CTkFrame(self, height=60, corner_radius=0)
        barra_sup.pack(fill="x", side="top", padx=0, pady=0)

        lbl_sede_titulo = ctk.CTkLabel(
            barra_sup,
            text=f"TERMINAL DISTRIBUIDA — {self.cliente.sede.upper()}",
            font=ctk.CTkFont(size=18, weight="bold"),
            text_color="#4da6ff",
        )
        lbl_sede_titulo.pack(side="left", padx=20, pady=15)

        btn_salir = ctk.CTkButton(
            barra_sup,
            text="Cerrar Sesión",
            width=120,
            fg_color="#802b2b",
            hover_color="#a63a3a",
            command=self._cerrar_sesion,
        )
        btn_salir.pack(side="right", padx=20, pady=15)

        # 2. Contenedor de notificaciones y alertas
        self.frame_banners = ctk.CTkFrame(self, fg_color="transparent")
        self.frame_banners.pack(fill="x", padx=0, pady=0)

        self.banner_alerta = ctk.CTkLabel(
            self.frame_banners,
            text="",
            height=30,
            fg_color="#aa3333",
            text_color="white",
            font=ctk.CTkFont(size=12, weight="bold"),
        )
        # Se oculta inicialmente

        # 3. Banner de notificación de eventos en tiempo real
        self.banner_evento = ctk.CTkLabel(
            self.frame_banners,
            text="",
            height=28,
            fg_color="#26643b",
            text_color="white",
            font=ctk.CTkFont(size=12),
        )
        # Se oculta inicialmente

        # 4. Pestañas principales
        self.tabview = ctk.CTkTabview(self)
        self.tabview.pack(fill="both", expand=True, padx=15, pady=(10, 5))

        tab_inv = self.tabview.add("Inventario de Productos")
        tab_hist = self.tabview.add("Historial de Movimientos")

        self._construir_tab_inventario(tab_inv)
        self._construir_tab_historial(tab_hist)

        # 5. Barra de estado inferior
        self.barra_estado = ctk.CTkFrame(self, height=32, corner_radius=0)
        self.barra_estado.pack(fill="x", side="bottom", padx=0, pady=0)

        self.lbl_estado_conexion = ctk.CTkLabel(
            self.barra_estado,
            text="● Conectado",
            text_color="#44cc44",
            font=ctk.CTkFont(size=12, weight="bold"),
        )
        self.lbl_estado_conexion.pack(side="left", padx=(15, 10), pady=6)

        self.lbl_info_usuario = ctk.CTkLabel(
            self.barra_estado,
            text=f"Servidor: {self.cliente._host}:{self.cliente._puerto} | Usuario: {self.cliente.usuario} | Rol: {self.cliente.rol}",
            font=ctk.CTkFont(size=11),
            text_color="gray",
        )
        self.lbl_info_usuario.pack(side="left", padx=10, pady=6)

        self.lbl_ultima_act = ctk.CTkLabel(
            self.barra_estado,
            text="Última actualización: --:--:--",
            font=ctk.CTkFont(size=11),
            text_color="gray",
        )
        self.lbl_ultima_act.pack(side="right", padx=15, pady=6)

    def _construir_tab_inventario(self, parent):
        # Barra de búsqueda y refresco
        barra_herramientas = ctk.CTkFrame(parent, fg_color="transparent")
        barra_herramientas.pack(fill="x", padx=10, pady=(5, 10))

        lbl_filtro = ctk.CTkLabel(barra_herramientas, text="Buscar por ID/Nombre:")
        lbl_filtro.pack(side="left", padx=(0, 10))

        self.entry_filtro = ctk.CTkEntry(barra_herramientas, width=240, placeholder_text="Filtrar catálogo...")
        self.entry_filtro.pack(side="left", padx=(0, 10))
        self.entry_filtro.bind("<KeyRelease>", lambda e: self._aplicar_filtro_tabla())

        self.btn_refrescar = ctk.CTkButton(
            barra_herramientas,
            text="Actualizar",
            width=100,
            command=self._cargar_inventario,
        )
        self.btn_refrescar.pack(side="left", padx=5)

        # Contenedor de la tabla Treeview
        frame_tabla = ctk.CTkFrame(parent)
        frame_tabla.pack(fill="both", expand=True, padx=10, pady=5)

        columnas = ("id", "nombre", "stock", "version")
        self.tree_inv = ttk.Treeview(
            frame_tabla,
            columns=columnas,
            show="headings",
            selectmode="browse",
        )

        self.tree_inv.heading("id", text="ID Producto")
        self.tree_inv.heading("nombre", text="Nombre del Producto")
        self.tree_inv.heading("stock", text="Stock en Existencia")
        self.tree_inv.heading("version", text="Versión Actual")

        self.tree_inv.column("id", width=140, anchor="center")
        self.tree_inv.column("nombre", width=420, anchor="w")
        self.tree_inv.column("stock", width=160, anchor="center")
        self.tree_inv.column("version", width=120, anchor="center")

        # Scrollbars
        scrollbar_y = ttk.Scrollbar(frame_tabla, orient="vertical", command=self.tree_inv.yview)
        self.tree_inv.configure(yscrollcommand=scrollbar_y.set)
        scrollbar_y.pack(side="right", fill="y")
        self.tree_inv.pack(fill="both", expand=True)

        # Configurar estilos de resaltado de eventos
        self.tree_inv.tag_configure("highlight", background="#1a4d2e", foreground="#ffffff")

        # Barra inferior de acciones sobre productos
        self.frame_acciones = ctk.CTkFrame(parent, fg_color="transparent")
        self.frame_acciones.pack(fill="x", padx=10, pady=(10, 5))

        self.btn_crear = ctk.CTkButton(self.frame_acciones, text="+ Crear Producto", command=self._abrir_dialogo_crear)
        self.btn_crear.pack(side="left", padx=5)

        self.btn_editar = ctk.CTkButton(self.frame_acciones, text="Editar Nombre", command=self._abrir_dialogo_editar)
        self.btn_editar.pack(side="left", padx=5)

        self.btn_ajustar = ctk.CTkButton(
            self.frame_acciones, text="Ajustar Stock (+/-)", fg_color="#2b7a78", command=self._abrir_dialogo_ajustar
        )
        self.btn_ajustar.pack(side="left", padx=5)

        self.btn_fijar = ctk.CTkButton(
            self.frame_acciones, text="Fijar Stock", fg_color="#17252a", command=self._abrir_dialogo_fijar
        )
        self.btn_fijar.pack(side="left", padx=5)

        self.btn_eliminar = ctk.CTkButton(
            self.frame_acciones, text="Eliminar Producto", fg_color="#802b2b", hover_color="#a63a3a", command=self._accion_eliminar
        )
        self.btn_eliminar.pack(side="right", padx=5)

    def _construir_tab_historial(self, parent):
        barra_herramientas_hist = ctk.CTkFrame(parent, fg_color="transparent")
        barra_herramientas_hist.pack(fill="x", padx=10, pady=(5, 10))

        btn_act_hist = ctk.CTkButton(
            barra_herramientas_hist, text="Actualizar Historial", width=140, command=self._cargar_historial
        )
        btn_act_hist.pack(side="left")

        frame_tabla_h = ctk.CTkFrame(parent)
        frame_tabla_h.pack(fill="both", expand=True, padx=10, pady=5)

        cols_h = ("id", "fecha", "usuario", "sede", "operacion", "producto_id", "detalle", "exito")
        self.tree_hist = ttk.Treeview(frame_tabla_h, columns=cols_h, show="headings", selectmode="browse")

        self.tree_hist.heading("id", text="#")
        self.tree_hist.heading("fecha", text="Fecha y Hora")
        self.tree_hist.heading("usuario", text="Usuario")
        self.tree_hist.heading("sede", text="Sede")
        self.tree_hist.heading("operacion", text="Operación")
        self.tree_hist.heading("producto_id", text="Producto ID")
        self.tree_hist.heading("detalle", text="Detalle de Operación")
        self.tree_hist.heading("exito", text="Resultado")

        self.tree_hist.column("id", width=40, anchor="center")
        self.tree_hist.column("fecha", width=140, anchor="center")
        self.tree_hist.column("usuario", width=100, anchor="center")
        self.tree_hist.column("sede", width=110, anchor="center")
        self.tree_hist.column("operacion", width=120, anchor="center")
        self.tree_hist.column("producto_id", width=100, anchor="center")
        self.tree_hist.column("detalle", width=340, anchor="w")
        self.tree_hist.column("exito", width=90, anchor="center")

        scroll_h = ttk.Scrollbar(frame_tabla_h, orient="vertical", command=self.tree_hist.yview)
        self.tree_hist.configure(yscrollcommand=scroll_h.set)
        scroll_h.pack(side="right", fill="y")
        self.tree_hist.pack(fill="both", expand=True)

    # ==========================================================================
    # CARGA Y ACTUALIZACIÓN DE DATOS (EN HILOS SEPARADOS)
    # ==========================================================================

    def _cargar_inventario(self):
        """Consulta el catálogo en segundo plano y actualiza la tabla."""
        def _tarea():
            try:
                resp = self.cliente.listar_productos()
                try:
                    self.after(0, self._renderizar_inventario, resp.products)
                except Exception:
                    pass
            except ErrorInventario as e:
                try:
                    self.after(0, self._manejar_error_red, e)
                except Exception:
                    pass

        threading.Thread(target=_tarea, daemon=True).start()

    def _renderizar_inventario(self, productos):
        """Renderiza los productos en la tabla Treeview."""
        self._productos_cache = {p.id: p for p in productos}
        self._aplicar_filtro_tabla()
        self._actualizar_timestamp()

    def _aplicar_filtro_tabla(self):
        """Filtra y muestra productos en el Treeview según el texto ingresado."""
        filtro = self.entry_filtro.get().strip().lower()

        for item in self.tree_inv.get_children():
            self.tree_inv.delete(item)

        for pid in sorted(self._productos_cache.keys()):
            p = self._productos_cache[pid]
            if not filtro or filtro in p.id.lower() or filtro in p.name.lower():
                self.tree_inv.insert("", "end", iid=p.id, values=(p.id, p.name, p.stock, p.version))

    def _cargar_historial(self):
        """Consulta los últimos movimientos en segundo plano."""
        def _tarea():
            try:
                resp = self.cliente.listar_movimientos(limite=50)
                try:
                    self.after(0, self._renderizar_historial, resp.movements)
                except Exception:
                    pass
            except ErrorInventario as e:
                try:
                    self.after(0, self._manejar_error_red, e)
                except Exception:
                    pass

        threading.Thread(target=_tarea, daemon=True).start()

    def _renderizar_historial(self, movimientos):
        for item in self.tree_hist.get_children():
            self.tree_hist.delete(item)

        for m in movimientos:
            res_txt = "ÉXITO" if m.exito else "FALLO"
            self.tree_hist.insert(
                "",
                "end",
                values=(m.id, m.fecha, m.usuario, m.sede, m.operacion, m.producto_id, m.detalle, res_txt),
            )

    def _actualizar_timestamp(self):
        hora_str = time.strftime("%H:%M:%S")
        self.lbl_ultima_act.configure(text=f"Última actualización: {hora_str}")

    # ==========================================================================
    # STREAMING DE EVENTOS EN TIEMPO REAL
    # ==========================================================================

    def _iniciar_streaming(self):
        """Suscribe el cliente al canal en vivo de WatchProducts."""
        self.cliente.suscribir(
            callback_evento=self._al_recibir_evento_stream,
            callback_estado=self._al_cambiar_estado_stream,
        )

    def _al_recibir_evento_stream(self, evento: inventory_pb2.ProductEvent):
        """Recibe el evento desde el hilo de gRPC y lo despacha a la UI en el hilo principal."""
        self.after(0, self._procesar_evento_remoto, evento)

    def _procesar_evento_remoto(self, evento: inventory_pb2.ProductEvent):
        """Aplica el cambio en la tabla, resalta la fila y muestra banner temporal."""
        p = evento.product
        pid = p.id
        tipo_str = {0: "creó", 1: "actualizó", 2: "eliminó"}.get(evento.tipo, "modificó")
        mensaje = f"{evento.origen_sede} {tipo_str} el producto {pid} ({p.name}) [v{p.version}]."

        # Mostrar notificación flotante
        self.banner_evento.configure(text=f"● {mensaje}")
        self.banner_evento.pack(fill="x", padx=15, pady=2)
        self.after(4500, lambda: self.banner_evento.pack_forget())

        if evento.tipo == inventory_pb2.EventType.DELETED:
            self._productos_cache.pop(pid, None)
            if self.tree_inv.exists(pid):
                self.tree_inv.delete(pid)
        else:
            self._productos_cache[pid] = p
            if self.tree_inv.exists(pid):
                self.tree_inv.item(pid, values=(p.id, p.name, p.stock, p.version), tags=("highlight",))
            else:
                self.tree_inv.insert("", "end", iid=pid, values=(p.id, p.name, p.stock, p.version), tags=("highlight",))

            # Quitar resaltado tras 3.5 segundos
            self.after(3500, lambda: self.tree_inv.exists(pid) and self.tree_inv.item(pid, tags=()))

        self._actualizar_timestamp()

    def _al_cambiar_estado_stream(self, estado: str, detalle: str):
        """Actualiza la barra de estado y maneja cortes de conexión."""
        self.after(0, self._actualizar_estado_red, estado, detalle)

    def _actualizar_estado_red(self, estado: str, detalle: str):
        if estado == "conectado":
            self.lbl_estado_conexion.configure(text="● Conectado", text_color="#44cc44")
            self.banner_alerta.pack_forget()
            self._habilitar_acciones(True)
            if not self._servidor_activo:
                # El servidor se recuperó: refrescar datos automáticamente
                self._servidor_activo = True
                self._cargar_inventario()
                self._cargar_historial()

        elif estado == "reconectando":
            self._servidor_activo = False
            self.lbl_estado_conexion.configure(text="● Reconectando...", text_color="#ffaa00")
            self.banner_alerta.configure(
                text="⚠ SERVIDOR CENTRAL DESCONECTADO — Intentando reconectar automáticamente..."
            )
            self.banner_alerta.pack(fill="x", padx=0, pady=0, side="top")
            self._habilitar_acciones(False)

        elif estado == "desconectado":
            self.lbl_estado_conexion.configure(text="● Desconectado", text_color="#ff4444")
            self._habilitar_acciones(False)

    def _habilitar_acciones(self, habilitar: bool):
        """Habilita o deshabilita botones de mutación durante cortes de red."""
        estado = "normal" if habilitar else "disabled"
        self.btn_crear.configure(state=estado)
        self.btn_editar.configure(state=estado)
        self.btn_ajustar.configure(state=estado)
        self.btn_fijar.configure(state=estado)
        self.btn_eliminar.configure(state=estado)
        self.btn_refrescar.configure(state=estado)

    def _manejar_error_red(self, error: ErrorInventario):
        """Muestra errores de red de forma no invasiva sin cerrar la ventana."""
        self.banner_alerta.configure(text=f"Error de red: {error}")
        self.banner_alerta.pack(fill="x", padx=0, pady=0, side="top")
        self.after(6000, lambda: self.banner_alerta.pack_forget())

    # ==========================================================================
    # DIÁLOGOS DE OPERACIÓN
    # ==========================================================================

    def _obtener_seleccionado(self) -> Optional[inventory_pb2.Product]:
        sel = self.tree_inv.selection()
        if not sel:
            messagebox.showwarning("Selección requerida", "Por favor seleccione un producto de la tabla.")
            return None
        pid = sel[0]
        return self._productos_cache.get(pid)

    def _abrir_dialogo_crear(self):
        DialogoCrearProducto(self, self.cliente, on_exito=self._cargar_inventario)

    def _abrir_dialogo_editar(self):
        prod = self._obtener_seleccionado()
        if prod:
            DialogoEditarProducto(self, self.cliente, prod, on_exito=self._cargar_inventario, on_conflicto=self._manejar_conflicto)

    def _abrir_dialogo_ajustar(self):
        prod = self._obtener_seleccionado()
        if prod:
            DialogoAjustarStock(self, self.cliente, prod, on_exito=self._cargar_inventario, on_conflicto=self._manejar_conflicto)

    def _abrir_dialogo_fijar(self):
        prod = self._obtener_seleccionado()
        if prod:
            DialogoFijarStock(self, self.cliente, prod, on_exito=self._cargar_inventario, on_conflicto=self._manejar_conflicto)

    def _accion_eliminar(self):
        prod = self._obtener_seleccionado()
        if not prod:
            return

        confirmar = messagebox.askyesno(
            "Confirmar Eliminación",
            f"¿Está seguro de que desea eliminar el producto '{prod.name}' (ID: {prod.id})?\n\nEsta acción es irreversible.",
        )
        if not confirmar:
            return

        def _tarea():
            try:
                resp = self.cliente.eliminar_producto(prod.id, expected_version=prod.version)
                if resp.success:
                    self.after(0, self._cargar_inventario)
                elif resp.version_conflict:
                    self.after(0, self._manejar_conflicto, prod.id, resp.error_message, resp.product)
                else:
                    self.after(0, lambda: messagebox.showerror("Operación Rechazada", resp.error_message))
            except ErrorInventario as e:
                self.after(0, lambda: messagebox.showerror("Error de Red", str(e)))

        threading.Thread(target=_tarea, daemon=True).start()

    def _manejar_conflicto(self, product_id: str, mensaje: str, prod_actualizado: Optional[inventory_pb2.Product]):
        """Explica el conflicto al usuario y actualiza la fila."""
        if prod_actualizado:
            self._productos_cache[prod_actualizado.id] = prod_actualizado
            self._aplicar_filtro_tabla()

        DialogoConflicto(self, product_id, mensaje, prod_actualizado, on_reintentar=self._cargar_inventario)

    def _cerrar_sesion(self):
        """Cierra el streaming, desconecta el cliente y regresa a la pantalla de login."""
        self.cliente.cerrar()
        self.master_app.mostrar_login()


# ==============================================================================
# MODALES Y DIÁLOGOS DE ENTRADA
# ==============================================================================

class DialogoCrearProducto(ctk.CTkToplevel):
    def __init__(self, master, cliente: ClienteInventario, on_exito):
        super().__init__(master)
        self.cliente = cliente
        self.on_exito = on_exito
        self.title("Crear Nuevo Producto")
        self.geometry("380x320")
        self.resizable(False, False)
        self.grab_set()

        ctk.CTkLabel(self, text="ID de Producto:").pack(anchor="w", padx=25, pady=(20, 2))
        self.entry_id = ctk.CTkEntry(self, width=330, placeholder_text="ej. PROD-006")
        self.entry_id.pack(padx=25, pady=2)

        ctk.CTkLabel(self, text="Nombre del Producto:").pack(anchor="w", padx=25, pady=(10, 2))
        self.entry_name = ctk.CTkEntry(self, width=330, placeholder_text="ej. Disco Duro SSD 1TB")
        self.entry_name.pack(padx=25, pady=2)

        ctk.CTkLabel(self, text="Stock Inicial:").pack(anchor="w", padx=25, pady=(10, 2))
        self.entry_stock = ctk.CTkEntry(self, width=330, placeholder_text="ej. 25")
        self.entry_stock.pack(padx=25, pady=2)

        self.btn_guardar = ctk.CTkButton(self, text="Guardar Producto", command=self._guardar)
        self.btn_guardar.pack(padx=25, pady=(25, 10), fill="x")

    def _guardar(self):
        pid = self.entry_id.get().strip()
        name = self.entry_name.get().strip()
        stock_str = self.entry_stock.get().strip()

        if not pid or not name or not stock_str:
            messagebox.showwarning("Campos Requeridos", "Todos los campos son obligatorios.")
            return

        try:
            stock = int(stock_str)
            if stock < 0:
                raise ValueError()
        except ValueError:
            messagebox.showerror("Dato Inválido", "El stock debe ser un número entero no negativo.")
            return

        self.btn_guardar.configure(state="disabled")

        def _tarea():
            try:
                resp = self.cliente.crear_producto(pid, name, stock)
                if resp.success:
                    self.after(0, self._exito)
                else:
                    self.after(0, lambda: messagebox.showerror("Operación Rechazada", resp.error_message))
                    self.after(0, lambda: self.btn_guardar.configure(state="normal"))
            except ErrorInventario as e:
                self.after(0, lambda: messagebox.showerror("Error", str(e)))
                self.after(0, lambda: self.btn_guardar.configure(state="normal"))

        threading.Thread(target=_tarea, daemon=True).start()

    def _exito(self):
        self.destroy()
        self.on_exito()


class DialogoEditarProducto(ctk.CTkToplevel):
    def __init__(self, master, cliente: ClienteInventario, prod: inventory_pb2.Product, on_exito, on_conflicto):
        super().__init__(master)
        self.cliente = cliente
        self.prod = prod
        self.on_exito = on_exito
        self.on_conflicto = on_conflicto
        self.title("Editar Nombre de Producto")
        self.geometry("380x260")
        self.resizable(False, False)
        self.grab_set()

        ctk.CTkLabel(self, text=f"Producto: {prod.id} (Versión esperada: {prod.version})", text_color="gray").pack(
            anchor="w", padx=25, pady=(20, 2)
        )
        ctk.CTkLabel(self, text="Nuevo Nombre:").pack(anchor="w", padx=25, pady=(10, 2))
        self.entry_name = ctk.CTkEntry(self, width=330)
        self.entry_name.insert(0, prod.name)
        self.entry_name.pack(padx=25, pady=2)

        self.btn_guardar = ctk.CTkButton(self, text="Guardar Cambios", command=self._guardar)
        self.btn_guardar.pack(padx=25, pady=(30, 10), fill="x")

    def _guardar(self):
        nuevo_nombre = self.entry_name.get().strip()
        if not nuevo_nombre:
            messagebox.showwarning("Campo Vacío", "El nombre no puede estar vacío.")
            return

        self.btn_guardar.configure(state="disabled")

        def _tarea():
            try:
                resp = self.cliente.actualizar_producto(self.prod.id, nuevo_nombre, expected_version=self.prod.version)
                if resp.success:
                    self.after(0, self._exito)
                elif resp.version_conflict:
                    self.after(0, self.destroy)
                    self.after(0, self.on_conflicto, self.prod.id, resp.error_message, resp.product)
                else:
                    self.after(0, lambda: messagebox.showerror("Operación Rechazada", resp.error_message))
                    self.after(0, lambda: self.btn_guardar.configure(state="normal"))
            except ErrorInventario as e:
                self.after(0, lambda: messagebox.showerror("Error", str(e)))
                self.after(0, lambda: self.btn_guardar.configure(state="normal"))

        threading.Thread(target=_tarea, daemon=True).start()

    def _exito(self):
        self.destroy()
        self.on_exito()


class DialogoAjustarStock(ctk.CTkToplevel):
    def __init__(self, master, cliente: ClienteInventario, prod: inventory_pb2.Product, on_exito, on_conflicto):
        super().__init__(master)
        self.cliente = cliente
        self.prod = prod
        self.on_exito = on_exito
        self.on_conflicto = on_conflicto
        self.title("Ajustar Stock (+/-)")
        self.geometry("380x280")
        self.resizable(False, False)
        self.grab_set()

        info = f"{prod.name} (ID: {prod.id})\nStock actual: {prod.stock} | Versión esperada: {prod.version}"
        ctk.CTkLabel(self, text=info, text_color="gray", justify="left").pack(anchor="w", padx=25, pady=(20, 5))

        ctk.CTkLabel(self, text="Ajuste (+N sumar, -N restar):").pack(anchor="w", padx=25, pady=(10, 2))
        self.entry_delta = ctk.CTkEntry(self, width=330, placeholder_text="ej. +5 o -3")
        self.entry_delta.pack(padx=25, pady=2)

        self.btn_guardar = ctk.CTkButton(self, text="Aplicar Ajuste", command=self._guardar)
        self.btn_guardar.pack(padx=25, pady=(25, 10), fill="x")

    def _guardar(self):
        delta_str = self.entry_delta.get().strip()
        try:
            delta = int(delta_str)
            if delta == 0:
                raise ValueError()
        except ValueError:
            messagebox.showerror("Dato Inválido", "Ingrese un número entero positivo o negativo distinto de 0.")
            return

        self.btn_guardar.configure(state="disabled")

        def _tarea():
            try:
                resp = self.cliente.ajustar_stock(self.prod.id, delta, expected_version=self.prod.version)
                if resp.success:
                    self.after(0, self._exito)
                elif resp.version_conflict:
                    self.after(0, self.destroy)
                    self.after(0, self.on_conflicto, self.prod.id, resp.error_message, resp.product)
                else:
                    self.after(0, lambda: messagebox.showerror("Operación Rechazada", resp.error_message))
                    self.after(0, lambda: self.btn_guardar.configure(state="normal"))
            except ErrorInventario as e:
                self.after(0, lambda: messagebox.showerror("Error", str(e)))
                self.after(0, lambda: self.btn_guardar.configure(state="normal"))

        threading.Thread(target=_tarea, daemon=True).start()

    def _exito(self):
        self.destroy()
        self.on_exito()


class DialogoFijarStock(ctk.CTkToplevel):
    def __init__(self, master, cliente: ClienteInventario, prod: inventory_pb2.Product, on_exito, on_conflicto):
        super().__init__(master)
        self.cliente = cliente
        self.prod = prod
        self.on_exito = on_exito
        self.on_conflicto = on_conflicto
        self.title("Fijar Cantidad de Stock")
        self.geometry("380x280")
        self.resizable(False, False)
        self.grab_set()

        info = f"{prod.name} (ID: {prod.id})\nStock actual: {prod.stock} | Versión esperada: {prod.version}"
        ctk.CTkLabel(self, text=info, text_color="gray", justify="left").pack(anchor="w", padx=25, pady=(20, 5))

        ctk.CTkLabel(self, text="Nueva Cantidad Exacta:").pack(anchor="w", padx=25, pady=(10, 2))
        self.entry_stock = ctk.CTkEntry(self, width=330, placeholder_text="ej. 50")
        self.entry_stock.pack(padx=25, pady=2)

        self.btn_guardar = ctk.CTkButton(self, text="Fijar Stock", command=self._guardar)
        self.btn_guardar.pack(padx=25, pady=(25, 10), fill="x")

    def _guardar(self):
        stock_str = self.entry_stock.get().strip()
        try:
            nuevo_stock = int(stock_str)
            if nuevo_stock < 0:
                raise ValueError()
        except ValueError:
            messagebox.showerror("Dato Inválido", "El stock debe ser un entero no negativo.")
            return

        self.btn_guardar.configure(state="disabled")

        def _tarea():
            try:
                resp = self.cliente.establecer_stock(self.prod.id, nuevo_stock, expected_version=self.prod.version)
                if resp.success:
                    self.after(0, self._exito)
                elif resp.version_conflict:
                    self.after(0, self.destroy)
                    self.after(0, self.on_conflicto, self.prod.id, resp.error_message, resp.product)
                else:
                    self.after(0, lambda: messagebox.showerror("Operación Rechazada", resp.error_message))
                    self.after(0, lambda: self.btn_guardar.configure(state="normal"))
            except ErrorInventario as e:
                self.after(0, lambda: messagebox.showerror("Error", str(e)))
                self.after(0, lambda: self.btn_guardar.configure(state="normal"))

        threading.Thread(target=_tarea, daemon=True).start()

    def _exito(self):
        self.destroy()
        self.on_exito()


class DialogoConflicto(ctk.CTkToplevel):
    """Diálogo explicativo de conflicto de concurrencia optimista."""

    def __init__(self, master, product_id: str, mensaje: str, prod_nuevo: Optional[inventory_pb2.Product], on_reintentar):
        super().__init__(master)
        self.on_reintentar = on_reintentar
        self.title("Conflicto de Concurrencia Optimista")
        self.geometry("450x290")
        self.resizable(False, False)
        self.grab_set()

        lbl_icono = ctk.CTkLabel(
            self,
            text="⚠ CONFLICTO DE VERSIÓN DETECTADO",
            text_color="#ffbb33",
            font=ctk.CTkFont(size=15, weight="bold"),
        )
        lbl_icono.pack(padx=20, pady=(20, 10))

        explicacion = (
            f"El producto '{product_id}' fue modificado por otra sede simultáneamente mientras usted operaba.\n\n"
            f"Detalle del servidor:\n{mensaje}\n\n"
            "La fila ha sido sincronizada con el estado más reciente del servidor central."
        )
        lbl_texto = ctk.CTkLabel(self, text=explicacion, wraplength=400, justify="left")
        lbl_texto.pack(padx=20, pady=5)

        btn_ok = ctk.CTkButton(self, text="Entendido, actualizar tabla", command=self._cerrar)
        btn_ok.pack(padx=20, pady=(20, 15), fill="x")

    def _cerrar(self):
        self.destroy()
        self.on_reintentar()


# ==============================================================================
# 3. PANTALLA PROVISIONAL PARA ROL SIMULADOR (ENLACE PARA TAREA 7)
# ==============================================================================

class FrameSimuladorPlaceholder(ctk.CTkFrame):
    """Punto de enlace provisional para la interfaz del Simulador (Tarea 7)."""

    def __init__(self, master: AppInventario, cliente: ClienteInventario):
        super().__init__(master)
        self.master_app = master
        self.cliente = cliente

        # TODO (Tarea 7): Reemplazar este placeholder por el panel interactivo completo
        # del simulador de concurrencia (disparo multihilo, gráficos/métricas y reportes).

        lbl_tit = ctk.CTkLabel(
            self,
            text="PANEL DEL SIMULADOR DE CONCURRENCIA",
            font=ctk.CTkFont(size=20, weight="bold"),
        )
        lbl_tit.pack(padx=30, pady=(60, 10))

        lbl_info = ctk.CTkLabel(
            self,
            text=(
                f"Sesión activa: {self.cliente.usuario} ({self.cliente.sede}) [Rol: {self.cliente.rol}]\n\n"
                "La interfaz visual avanzada del simulador será implementada en la Tarea 7.\n"
                "Por ahora, el enlace de autenticación e inicialización se encuentra verificado."
            ),
            font=ctk.CTkFont(size=14),
            text_color="gray",
            justify="center",
        )
        lbl_info.pack(padx=30, pady=20)

        btn_salir = ctk.CTkButton(
            self,
            text="Cerrar Sesión",
            width=160,
            command=self._cerrar_sesion,
        )
        btn_salir.pack(pady=20)

    def _cerrar_sesion(self):
        self.cliente.cerrar()
        self.master_app.mostrar_login()


# ==============================================================================
# PUNTO DE ENTRADA CLI/GUI
# ==============================================================================

def main():
    """Función de arranque para python -m src.client.cliente_gui."""
    app = AppInventario()
    app.mainloop()


if __name__ == "__main__":
    main()

