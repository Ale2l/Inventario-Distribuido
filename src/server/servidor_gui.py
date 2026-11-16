"""
Interfaz gráfica interactiva para el Servidor Central de InventarioDistribuido (CustomTkinter).

Permite administrar el ciclo de vida del servidor gRPC (arranque/detención sin bloquear la GUI),
verificar la conectividad estricta con PostgreSQL, detectar y exponer todas las IPv4 locales
(incluyendo Radmin VPN y Wi-Fi), monitorear clientes conectados, auditar operaciones concurrentes
en tiempo real y supervisar el inventario central.
"""

import logging
import queue
import re
import socket
import subprocess
import threading
import time
from typing import List, Dict, Any, Optional
import tkinter as tk
from tkinter import ttk, messagebox

import customtkinter as ctk
import grpc

from src.config import (
    GRPC_SERVER_PORT,
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_USER,
)
from src.generated import inventory_pb2
from src.server.servidor import crear_servidor, DEFAULT_MAX_WORKERS
from src.server.bus_eventos import BusEventos
from src.server.interceptor import GestorSesiones
from src.storage.inventario_memoria import Producto

logger = logging.getLogger("ServidorGUI")

ctk.set_appearance_mode("Dark")
ctk.set_default_color_theme("blue")


def obtener_direcciones_red(puerto: int = GRPC_SERVER_PORT) -> List[Dict[str, Any]]:
    """
    Detecta todas las direcciones IPv4 locales no loopback del equipo.
    Identifica adaptadores de Radmin VPN, Wi-Fi y Ethernet.
    """
    direcciones: List[Dict[str, Any]] = []
    vistos = set()

    # 1. Intentar analizar la salida de ipconfig en Windows para obtener nombres de adaptadores
    try:
        salida = subprocess.check_output("ipconfig", text=True, errors="ignore")
        adaptador_actual = "Adaptador Desconocido"

        for linea in salida.splitlines():
            linea_s = linea.strip()
            if linea and not linea.startswith(" ") and ":" in linea:
                adaptador_actual = linea.split(":")[0].strip()
            elif "IPv4" in linea_s:
                partes = linea_s.split(":")
                if len(partes) > 1:
                    ip = partes[1].replace("(Preferido)", "").strip()
                    if ip and not ip.startswith("127.") and ip not in vistos:
                        # Clasificar el tipo de interfaz
                        adaptador_lower = adaptador_actual.lower()
                        if "radmin" in adaptador_lower or ip.startswith("26."):
                            tipo = "Radmin VPN"
                            prioridad = 1
                        elif "wi-fi" in adaptador_lower or "inal" in adaptador_lower:
                            tipo = "Wi-Fi"
                            prioridad = 2
                        elif "ethernet" in adaptador_lower:
                            tipo = "Ethernet / LAN"
                            prioridad = 3
                        else:
                            tipo = "Red Local"
                            prioridad = 4

                        direcciones.append({
                            "ip": ip,
                            "puerto": puerto,
                            "tipo": tipo,
                            "adaptador": adaptador_actual,
                            "prioridad": prioridad,
                        })
                        vistos.add(ip)
    except Exception as e:
        logger.warning(f"No se pudo consultar ipconfig: {e}")

    # 2. Respaldo mediante resolución de nombres por socket
    try:
        hostname = socket.gethostname()
        for ip in socket.gethostbyname_ex(hostname)[2]:
            if not ip.startswith("127.") and ip not in vistos:
                tipo = "Radmin VPN" if ip.startswith("26.") else "Red Local"
                direcciones.append({
                    "ip": ip,
                    "puerto": puerto,
                    "tipo": tipo,
                    "adaptador": "Socket Local",
                    "prioridad": 1 if ip.startswith("26.") else 3,
                })
                vistos.add(ip)
    except Exception:
        pass

    # Ordenar por prioridad (Radmin VPN y Wi-Fi primero)
    direcciones.sort(key=lambda d: d["prioridad"])
    return direcciones


class AppServidor(ctk.CTk):
    """Ventana principal del Centro de Control del Servidor Central."""

    def __init__(self):
        super().__init__()
        self.title("Centro de Control - Servidor Central gRPC (InventarioDistribuido)")
        self.geometry("1150x800")
        self.minsize(980, 700)

        # Estado del servidor y almacenamiento
        self._server: Optional[grpc.Server] = None
        self._almacen = None
        self._gestor_sesiones = GestorSesiones()
        self._bus_eventos = BusEventos()
        self._cola_eventos_gui: Optional[queue.Queue] = None

        self._hilo_escucha_eventos: Optional[threading.Thread] = None
        self._detener_hilos = threading.Event()
        self._total_operaciones = 0

        self._crear_interfaz()
        self._actualizar_lista_ips()

        # Iniciar ciclo de actualización periódica
        self.after(2000, self._actualizacion_periodica)

        # Cierre ordenado
        self.protocol("WM_DELETE_WINDOW", self.al_cerrar_aplicacion)

    def _crear_interfaz(self):
        # ======================================================================
        # 1. BARRA SUPERIOR DE CONTROL
        # ======================================================================
        barra_sup = ctk.CTkFrame(self, height=75, corner_radius=0)
        barra_sup.pack(fill="x", side="top", padx=0, pady=0)

        f_tit = ctk.CTkFrame(barra_sup, fg_color="transparent")
        f_tit.pack(side="left", padx=20, pady=10)

        lbl_tit = ctk.CTkLabel(
            f_tit,
            text="SERVIDOR CENTRAL DISTRIBUIDO",
            font=ctk.CTkFont(size=18, weight="bold"),
            text_color="#38bdf8",
        )
        lbl_tit.pack(anchor="w")

        lbl_sub = ctk.CTkLabel(
            f_tit,
            text=f"gRPC Server Engine | Base: {DB_NAME} en {DB_HOST}:{DB_PORT}",
            font=ctk.CTkFont(size=12),
            text_color="gray",
        )
        lbl_sub.pack(anchor="w")

        # Controles de Arranque/Detención
        f_ctrl = ctk.CTkFrame(barra_sup, fg_color="transparent")
        f_ctrl.pack(side="right", padx=20, pady=10)

        self.lbl_indicador_estado = ctk.CTkLabel(
            f_ctrl,
            text="● Servidor Detenido",
            font=ctk.CTkFont(size=13, weight="bold"),
            text_color="#ef4444",
        )
        self.lbl_indicador_estado.pack(side="left", padx=15)

        self.btn_iniciar = ctk.CTkButton(
            f_ctrl,
            text="▶ Iniciar Servidor",
            font=ctk.CTkFont(size=13, weight="bold"),
            fg_color="#16a34a",
            hover_color="#15803d",
            width=140,
            command=self._iniciar_servidor_click,
        )
        self.btn_iniciar.pack(side="left", padx=5)

        self.btn_detener = ctk.CTkButton(
            f_ctrl,
            text="⏹ Detener Servidor",
            font=ctk.CTkFont(size=13, weight="bold"),
            fg_color="#dc2626",
            hover_color="#b91c1c",
            width=140,
            state="disabled",
            command=self._detener_servidor_click,
        )
        self.btn_detener.pack(side="left", padx=5)

        # ======================================================================
        # 2. PANEL DE DIRECCIONES IPv4 (PARA QUE LOS CLIENTES SE CONECTEN)
        # ======================================================================
        self.frame_ips = ctk.CTkFrame(self, corner_radius=10)
        self.frame_ips.pack(fill="x", padx=15, pady=(10, 5))

        f_ip_tit = ctk.CTkFrame(self.frame_ips, fg_color="transparent")
        f_ip_tit.pack(fill="x", padx=15, pady=(8, 4))

        lbl_ip_header = ctk.CTkLabel(
            f_ip_tit,
            text="🌐 DIRECCIONES IPv4 DEL SERVIDOR (Copiar para configurar clientes en otras máquinas):",
            font=ctk.CTkFont(size=12, weight="bold"),
            text_color="#94a3b8",
        )
        lbl_ip_header.pack(side="left")

        btn_recargar_ips = ctk.CTkButton(
            f_ip_tit,
            text="⟳ Recargar Red",
            width=100,
            height=24,
            font=ctk.CTkFont(size=11),
            command=self._actualizar_lista_ips,
        )
        btn_recargar_ips.pack(side="right")

        self.contenedor_tarjetas_ip = ctk.CTkFrame(self.frame_ips, fg_color="transparent")
        self.contenedor_tarjetas_ip.pack(fill="x", padx=15, pady=(2, 10))

        # ======================================================================
        # 3. PESTAÑAS PRINCIPALES: INVENTARIO / SESIONES / LOG EN VIVO
        # ======================================================================
        self.tabview = ctk.CTkTabview(self)
        self.tabview.pack(fill="both", expand=True, padx=15, pady=5)

        tab_inv = self.tabview.add("Catálogo de Inventario")
        tab_log = self.tabview.add("Monitor de Operaciones en Vivo")
        tab_ses = self.tabview.add("Clientes Conectados (Sesiones)")

        self._construir_tab_inventario(tab_inv)
        self._construir_tab_log(tab_log)
        self._construir_tab_sesiones(tab_ses)

        # ======================================================================
        # 4. BARRA DE ESTADO INFERIOR
        # ======================================================================
        barra_inf = ctk.CTkFrame(self, height=34, corner_radius=0)
        barra_inf.pack(fill="x", side="bottom", padx=0, pady=0)

        self.lbl_estado_bd = ctk.CTkLabel(
            barra_inf,
            text="PostgreSQL: Desconectado",
            font=ctk.CTkFont(size=11),
            text_color="#ef4444",
        )
        self.lbl_estado_bd.pack(side="left", padx=15, pady=6)

        self.lbl_cont_sesiones = ctk.CTkLabel(
            barra_inf,
            text="Clientes activos: 0",
            font=ctk.CTkFont(size=11),
            text_color="gray",
        )
        self.lbl_cont_sesiones.pack(side="left", padx=15, pady=6)

        self.lbl_cont_ops = ctk.CTkLabel(
            barra_inf,
            text="Operaciones procesadas: 0",
            font=ctk.CTkFont(size=11),
            text_color="gray",
        )
        self.lbl_cont_ops.pack(side="right", padx=15, pady=6)

    # ==========================================================================
    # CONSTRUCCIÓN DE PESTAÑAS
    # ==========================================================================

    def _construir_tab_inventario(self, parent):
        # Barra superior con botón de recarga
        f_top = ctk.CTkFrame(parent, fg_color="transparent")
        f_top.pack(fill="x", padx=10, pady=(5, 5))

        lbl_desc = ctk.CTkLabel(
            f_top,
            text="Estado actual en base de datos central (Actualizado automáticamente):",
            font=ctk.CTkFont(size=12),
            text_color="gray",
        )
        lbl_desc.pack(side="left")

        btn_refrescar_inv = ctk.CTkButton(
            f_top,
            text="Actualizar Ahora",
            width=120,
            command=self._cargar_inventario_central,
        )
        btn_refrescar_inv.pack(side="right")

        # Tabla Treeview
        frame_t = ctk.CTkFrame(parent)
        frame_t.pack(fill="both", expand=True, padx=10, pady=5)

        cols = ("id", "nombre", "stock", "version")
        self.tree_inv = ttk.Treeview(frame_t, columns=cols, show="headings", selectmode="browse")
        self.tree_inv.heading("id", text="ID Producto")
        self.tree_inv.heading("nombre", text="Nombre del Producto")
        self.tree_inv.heading("stock", text="Stock en Existencia")
        self.tree_inv.heading("version", text="Versión Actual")

        self.tree_inv.column("id", width=140, anchor="center")
        self.tree_inv.column("nombre", width=450, anchor="w")
        self.tree_inv.column("stock", width=160, anchor="center")
        self.tree_inv.column("version", width=120, anchor="center")

        scroll_y = ttk.Scrollbar(frame_t, orient="vertical", command=self.tree_inv.yview)
        self.tree_inv.configure(yscrollcommand=scroll_y.set)
        scroll_y.pack(side="right", fill="y")
        self.tree_inv.pack(fill="both", expand=True)

        self.tree_inv.tag_configure("highlight", background="#1a4d2e", foreground="#ffffff")

    def _construir_tab_log(self, parent):
        f_top = ctk.CTkFrame(parent, fg_color="transparent")
        f_top.pack(fill="x", padx=10, pady=(5, 5))

        lbl_desc = ctk.CTkLabel(
            f_top,
            text="Registro en vivo de llamadas gRPC entrantes y resultados de concurrencia:",
            font=ctk.CTkFont(size=12),
            text_color="gray",
        )
        lbl_desc.pack(side="left")

        btn_limpiar = ctk.CTkButton(
            f_top,
            text="Limpiar Historial",
            width=120,
            fg_color="#475569",
            hover_color="#334155",
            command=self._limpiar_log,
        )
        btn_limpiar.pack(side="right")

        frame_t = ctk.CTkFrame(parent)
        frame_t.pack(fill="both", expand=True, padx=10, pady=5)

        cols = ("hora", "sede", "operacion", "producto", "resultado", "detalle")
        self.tree_log = ttk.Treeview(frame_t, columns=cols, show="headings", selectmode="browse")

        self.tree_log.heading("hora", text="Hora")
        self.tree_log.heading("sede", text="Sede / Usuario")
        self.tree_log.heading("operacion", text="Operación")
        self.tree_log.heading("producto", text="Producto")
        self.tree_log.heading("resultado", text="Resultado")
        self.tree_log.heading("detalle", text="Detalle de Operación")

        self.tree_log.column("hora", width=90, anchor="center")
        self.tree_log.column("sede", width=150, anchor="center")
        self.tree_log.column("operacion", width=140, anchor="center")
        self.tree_log.column("producto", width=110, anchor="center")
        self.tree_log.column("resultado", width=170, anchor="center")
        self.tree_log.column("detalle", width=420, anchor="w")

        # Configuración de etiquetas y colores para resultados
        self.tree_log.tag_configure("exito", foreground="#4ade80")
        self.tree_log.tag_configure("conflicto", foreground="#facc15")
        self.tree_log.tag_configure("insuficiente", foreground="#fb923c")
        self.tree_log.tag_configure("error", foreground="#f87171")

        scroll_y = ttk.Scrollbar(frame_t, orient="vertical", command=self.tree_log.yview)
        self.tree_log.configure(yscrollcommand=scroll_y.set)
        scroll_y.pack(side="right", fill="y")
        self.tree_log.pack(fill="both", expand=True)

    def _construir_tab_sesiones(self, parent):
        f_top = ctk.CTkFrame(parent, fg_color="transparent")
        f_top.pack(fill="x", padx=10, pady=(5, 5))

        lbl_desc = ctk.CTkLabel(
            f_top,
            text="Clientes y sedes con sesión autenticada activa en el GestorSesiones:",
            font=ctk.CTkFont(size=12),
            text_color="gray",
        )
        lbl_desc.pack(side="left")

        frame_t = ctk.CTkFrame(parent)
        frame_t.pack(fill="both", expand=True, padx=10, pady=5)

        cols = ("usuario", "sede", "rol", "hora_login")
        self.tree_ses = ttk.Treeview(frame_t, columns=cols, show="headings", selectmode="browse")

        self.tree_ses.heading("usuario", text="Usuario")
        self.tree_ses.heading("sede", text="Sede Asignada")
        self.tree_ses.heading("rol", text="Rol")
        self.tree_ses.heading("hora_login", text="Hora de Conexión")

        self.tree_ses.column("usuario", width=160, anchor="center")
        self.tree_ses.column("sede", width=220, anchor="center")
        self.tree_ses.column("rol", width=140, anchor="center")
        self.tree_ses.column("hora_login", width=160, anchor="center")

        scroll_y = ttk.Scrollbar(frame_t, orient="vertical", command=self.tree_ses.yview)
        self.tree_ses.configure(yscrollcommand=scroll_y.set)
        scroll_y.pack(side="right", fill="y")
        self.tree_ses.pack(fill="both", expand=True)

    # ==========================================================================
    # GESTIÓN DE DIRECCIONES IP Y COPIADO AL PORTAPAPELES
    # ==========================================================================

    def _actualizar_lista_ips(self):
        """Detecta las IPv4 disponibles y dibuja tarjetas con botón de copiado."""
        for w in self.contenedor_tarjetas_ip.winfo_children():
            w.destroy()

        direcciones = obtener_direcciones_red(puerto=GRPC_SERVER_PORT)

        if not direcciones:
            lbl = ctk.CTkLabel(
                self.contenedor_tarjetas_ip,
                text="No se detectaron interfaces de red activas. Use '127.0.0.1:50051' localmente.",
                text_color="gray",
            )
            lbl.pack(side="left", padx=5)
            return

        for d in direcciones:
            card = ctk.CTkFrame(self.contenedor_tarjetas_ip, fg_color="#1e293b", corner_radius=8)
            card.pack(side="left", padx=6, pady=2)

            tipo_color = "#38bdf8" if "Radmin" in d["tipo"] else ("#34d399" if "Wi-Fi" in d["tipo"] else "#a78bfa")

            lbl_tag = ctk.CTkLabel(
                card,
                text=f"[{d['tipo']}]",
                font=ctk.CTkFont(size=10, weight="bold"),
                text_color=tipo_color,
            )
            lbl_tag.pack(side="left", padx=(10, 4), pady=4)

            cadena_conexion = f"{d['ip']}:{d['puerto']}"
            lbl_ip = ctk.CTkLabel(
                card,
                text=cadena_conexion,
                font=ctk.CTkFont(size=12, weight="bold"),
                text_color="white",
            )
            lbl_ip.pack(side="left", padx=(2, 8), pady=4)

            btn_copy = ctk.CTkButton(
                card,
                text="Copiar",
                width=55,
                height=22,
                font=ctk.CTkFont(size=11),
                command=lambda s=cadena_conexion: self._copiar_al_portapapeles(s),
            )
            btn_copy.pack(side="right", padx=(2, 8), pady=4)

    def _copiar_al_portapapeles(self, texto: str):
        self.clipboard_clear()
        self.clipboard_append(texto)
        messagebox.showinfo("Copiado al Portapapeles", f"Dirección copiada exitosamente:\n{texto}")

    # ==========================================================================
    # CICLO DE VIDA: INICIO Y DETENCIÓN DEL SERVIDOR gRPC
    # ==========================================================================

    def _iniciar_servidor_click(self):
        """Verifica la conectividad con PostgreSQL e inicia el servidor gRPC."""
        self.btn_iniciar.configure(state="disabled", text="Verificando BD...")

        def _trabajo():
            # 1. Verificación estricta de conexión con PostgreSQL
            try:
                from src.storage.inventario_postgres import InventarioPostgres
                almacen = InventarioPostgres()
                # Test de conexión real con timeout corto
                with almacen.pool.connection(timeout=3.0) as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1;")
            except Exception as e:
                logger.error(f"Error al conectar con PostgreSQL: {e}")
                try:
                    self.after(
                        0,
                        self._mostrar_error_inicio_bd,
                        (
                            f"No se pudo conectar a la base de datos PostgreSQL '{DB_NAME}' en {DB_HOST}:{DB_PORT}.\n\n"
                            f"Detalle del error:\n{e}\n\n"
                            f"Asegúrese de que el servicio de PostgreSQL esté activo y haya inicializado "
                            f"el esquema ejecutando: python scripts/init_db.py"
                        ),
                    )
                except Exception:
                    pass
                return

            # 2. Arranque del servidor gRPC usando crear_servidor()
            try:
                self._almacen = almacen
                self._cola_eventos_gui = self._bus_eventos.suscribir()

                self._server = crear_servidor(
                    port=GRPC_SERVER_PORT,
                    max_workers=DEFAULT_MAX_WORKERS,
                    almacen=self._almacen,
                    gestor_sesiones=self._gestor_sesiones,
                    bus_eventos=self._bus_eventos,
                    usar_interceptor=True,
                    callback_auditoria=self._on_operacion_servidor,
                )
                self._server.start()

                # Lanzar escucha de eventos en segundo plano
                self._detener_hilos.clear()
                self._hilo_escucha_eventos = threading.Thread(
                    target=self._hilo_escucha_bus, daemon=True
                )
                self._hilo_escucha_eventos.start()

                try:
                    self.after(0, self._notificar_servidor_iniciado)
                except Exception:
                    pass

            except Exception as e:
                logger.error(f"Error al iniciar servidor gRPC: {e}")
                if self._almacen and hasattr(self._almacen, "close"):
                    self._almacen.close()
                    self._almacen = None
                try:
                    self.after(0, self._mostrar_error_inicio_grpc, str(e))
                except Exception:
                    pass

        threading.Thread(target=_trabajo, daemon=True).start()

    def _notificar_servidor_iniciado(self):
        self.lbl_indicador_estado.configure(
            text=f"● SERVIDOR ACTIVO (Puerto {GRPC_SERVER_PORT})",
            text_color="#22c55e",
        )
        self.lbl_estado_bd.configure(
            text=f"PostgreSQL: Conectado ({DB_NAME})",
            text_color="#22c55e",
        )
        self.btn_iniciar.configure(state="disabled", text="▶ Iniciar Servidor")
        self.btn_detener.configure(state="normal")

        # Cargar catálogo inicial
        self._cargar_inventario_central()

    def _mostrar_error_inicio_bd(self, mensaje: str):
        self.btn_iniciar.configure(state="normal", text="▶ Iniciar Servidor")
        self.lbl_indicador_estado.configure(text="● Error de Conexión BD", text_color="#ef4444")
        self.lbl_estado_bd.configure(text="PostgreSQL: Error de Conexión", text_color="#ef4444")
        messagebox.showerror("Error de Base de Datos", mensaje)

    def _mostrar_error_inicio_grpc(self, mensaje: str):
        self.btn_iniciar.configure(state="normal", text="▶ Iniciar Servidor")
        self.lbl_indicador_estado.configure(text="● Error al Vincular Puerto", text_color="#ef4444")
        messagebox.showerror("Error al Iniciar gRPC", f"No se pudo iniciar el servidor gRPC:\n{mensaje}")

    def detener_servidor(self, bloqueante: bool = False, grace: float = 0.5):
        """Detiene de forma limpia el servidor gRPC y el pool de PostgreSQL."""
        def _trabajo():
            self._detener_hilos.set()

            if self._cola_eventos_gui is not None:
                try:
                    self._bus_eventos.desuscribir(self._cola_eventos_gui)
                except Exception:
                    pass
                self._cola_eventos_gui = None

            if self._server is not None:
                try:
                    self._server.stop(grace=grace)
                except Exception as e:
                    logger.error(f"Error al detener servidor gRPC: {e}")
                self._server = None

            if self._almacen is not None:
                try:
                    if hasattr(self._almacen, "close"):
                        self._almacen.close()
                except Exception as e:
                    logger.error(f"Error al cerrar pool de base de datos: {e}")
                self._almacen = None

            try:
                self.after(0, self._notificar_servidor_detenido)
            except Exception:
                pass

        if bloqueante:
            _trabajo()
            self._notificar_servidor_detenido()
        else:
            threading.Thread(target=_trabajo, daemon=True).start()

    def _detener_servidor_click(self):
        """Manejador del botón de interfaz gráfica para detener el servidor."""
        self.btn_detener.configure(state="disabled", text="Deteniendo...")
        self.detener_servidor(bloqueante=False)

    def _notificar_servidor_detenido(self):
        self.lbl_indicador_estado.configure(text="● Servidor Detenido", text_color="#ef4444")
        self.lbl_estado_bd.configure(text="PostgreSQL: Desconectado", text_color="#ef4444")
        self.btn_iniciar.configure(state="normal", text="▶ Iniciar Servidor")
        self.btn_detener.configure(state="disabled", text="⏹ Detener Servidor")
        self.lbl_cont_sesiones.configure(text="Clientes activos: 0")

        # Limpiar sesiones
        for item in self.tree_ses.get_children():
            self.tree_ses.delete(item)

    # ==========================================================================
    # AUDITORÍA EN VIVO Y TIEMPO REAL
    # ==========================================================================

    def _on_operacion_servidor(self, op_dict: Dict[str, Any]):
        """Callback invocado por InventoryServiceServicer ante cualquier operación."""
        self._total_operaciones += 1
        try:
            self.after(0, self._agregar_fila_log, op_dict)
        except Exception:
            pass

    def _agregar_fila_log(self, op: Dict[str, Any]):
        self.lbl_cont_ops.configure(text=f"Operaciones procesadas: {self._total_operaciones}")

        res_str = op.get("resultado")
        if not res_str:
            if op.get("exito"):
                res_str = "ÉXITO"
                tag = "exito"
            else:
                msg_low = (op.get("mensaje") or "").lower()
                det_low = (op.get("detalle") or "").lower()
                if "conflicto" in msg_low or "conflicto" in det_low:
                    res_str = "CONFLICTO VERSIÓN"
                    tag = "conflicto"
                elif "insuficiente" in msg_low or "insuficiente" in det_low:
                    res_str = "STOCK INSUFICIENTE"
                    tag = "insuficiente"
                else:
                    res_str = "ERROR / DENEGADO"
                    tag = "error"
        else:
            tag = "exito" if "ÉXITO" in res_str else ("conflicto" if "CONFLICTO" in res_str else "error")

        # Insertar al inicio de la tabla (más reciente primero)
        self.tree_log.insert(
            "",
            0,
            values=(
                op.get("hora", time.strftime("%H:%M:%S")),
                f"{op.get('sede', 'Desconocida')} ({op.get('usuario', '-')})",
                op.get("operacion", "-"),
                op.get("producto_id", "-"),
                res_str,
                op.get("detalle") or op.get("mensaje") or "",
            ),
            tags=(tag,),
        )

        # Limitar tamaño de la tabla a 300 entradas
        hijos = self.tree_log.get_children()
        if len(hijos) > 300:
            for item in hijos[300:]:
                self.tree_log.delete(item)

    def _limpiar_log(self):
        for item in self.tree_log.get_children():
            self.tree_log.delete(item)

    # ==========================================================================
    # ESCUCHA DE EVENTOS Y REFRESH DE INVENTARIO
    # ==========================================================================

    def _hilo_escucha_bus(self):
        """Escucha eventos del BusEventos y dispara actualización del inventario."""
        while not self._detener_hilos.is_set():
            if self._cola_eventos_gui is None:
                break
            try:
                evento = self._cola_eventos_gui.get(timeout=1.0)
                try:
                    self.after(0, self._cargar_inventario_central)
                except Exception:
                    pass
            except queue.Empty:
                continue
            except Exception:
                break

    def _cargar_inventario_central(self):
        """Consulta los productos actuales del almacén central y actualiza el Treeview."""
        if self._almacen is None:
            return

        def _trabajo():
            try:
                productos = self._almacen.list_products()
                try:
                    self.after(0, self._renderizar_tabla_inventario, productos)
                except Exception:
                    pass
            except Exception as e:
                logger.error(f"Error al listar inventario central: {e}")

        threading.Thread(target=_trabajo, daemon=True).start()

    def _renderizar_tabla_inventario(self, productos: List[Producto]):
        for item in self.tree_inv.get_children():
            self.tree_inv.delete(item)

        for p in productos:
            self.tree_inv.insert(
                "",
                "end",
                iid=p.id,
                values=(p.id, p.name, p.stock, p.version),
            )

    # ==========================================================================
    # ACTUALIZACIÓN PERIÓDICA: SESIONES ACTIVAS E INVENTARIO
    # ==========================================================================

    def _actualizacion_periodica(self):
        """Refresca las sesiones activas y valida el estado periódicamente."""
        if self._server is not None:
            sesiones = self._gestor_sesiones.listar_sesiones()
            self.lbl_cont_sesiones.configure(text=f"Clientes activos: {len(sesiones)}")

            # Refrescar tabla de sesiones
            for item in self.tree_ses.get_children():
                self.tree_ses.delete(item)

            for s in sesiones:
                h_str = time.strftime("%H:%M:%S", time.localtime(s.get("created_at", time.time())))
                self.tree_ses.insert(
                    "",
                    "end",
                    values=(
                        s.get("username", "-"),
                        s.get("sede_nombre", "-"),
                        s.get("rol", "-"),
                        h_str,
                    ),
                )

        # Planificar siguiente ciclo
        if not self._detener_hilos.is_set():
            self.after(2500, self._actualizacion_periodica)

    # ==========================================================================
    # CIERRE ORDENADO DE LA VENTANA
    # ==========================================================================

    def al_cerrar_aplicacion(self):
        """Detiene el servidor, cierra conexiones activas y destruye la ventana."""
        self.detener_servidor(bloqueante=True, grace=0)
        try:
            self.destroy()
        except Exception:
            pass


def main():
    """Punto de arranque GUI: python -m src.server.servidor_gui"""
    app = AppServidor()
    app.mainloop()


if __name__ == "__main__":
    main()
