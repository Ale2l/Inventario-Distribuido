"""
Módulo de configuración centralizada para InventarioDistribuido.

Define los valores por defecto requeridos para la conexión a PostgreSQL,
el servidor central y los clientes gRPC, así como los roles y usuarios autorizados.
Permite sobreescribir cualquier parámetro mediante variables de entorno para
facilitar el despliegue en múltiples equipos en red Wi-Fi o VPN Radmin.
"""

import os
from typing import Dict, Any
from src.roles import Rol, PERMISOS_POR_ROL

# ==============================================================================
# CONFIGURACIÓN DE POSTGRESQL (Servidor Central)
# ==============================================================================
# El servidor central es el único nodo que se conecta a la base de datos.
# Los clientes se comunican exclusivamente mediante gRPC.
DB_HOST: str = os.getenv("INVENTARIO_DB_HOST", "localhost")
DB_PORT: int = int(os.getenv("INVENTARIO_DB_PORT", "5432"))
DB_NAME: str = os.getenv("INVENTARIO_DB_NAME", "inventario_distribuido")
DB_USER: str = os.getenv("INVENTARIO_DB_USER", "inventario_app")
DB_PASSWORD: str = os.getenv("INVENTARIO_DB_PASSWORD", "Inv#Dist2026")
DB_TEST_NAME: str = os.getenv("INVENTARIO_DB_TEST_NAME", "inventario_test")

# Tamaño del pool de conexiones psycopg 3 para atender múltiples llamadas gRPC simultáneas
DB_POOL_MIN_SIZE: int = int(os.getenv("INVENTARIO_DB_POOL_MIN", "2"))
DB_POOL_MAX_SIZE: int = int(os.getenv("INVENTARIO_DB_POOL_MAX", "10"))

# ==============================================================================
# CONFIGURACIÓN DE gRPC
# ==============================================================================
# El servidor gRPC escucha en 0.0.0.0 para aceptar conexiones entrantes desde
# cualquier interfaz de red (LAN Wi-Fi o adaptador virtual de Radmin VPN).
GRPC_SERVER_HOST: str = os.getenv("GRPC_SERVER_HOST", "0.0.0.0")
GRPC_SERVER_PORT: int = int(os.getenv("GRPC_SERVER_PORT", "50051"))
GRPC_DEFAULT_CLIENT_HOST: str = os.getenv("GRPC_DEFAULT_CLIENT_HOST", "localhost")

# Timeout por defecto de 5 segundos para llamadas gRPC remotas
GRPC_TIMEOUT: float = float(os.getenv("GRPC_TIMEOUT", "5.0"))

# ==============================================================================
# USUARIOS INICIALES Y CREDENCIALES
# ==============================================================================
# Cuentas predefinidas según la matriz de roles y permisos del proyecto.
# En la base de datos se almacenan con hash bcrypt; aquí se definen las
# credenciales iniciales para la siembra (seeding) y validación en memoria.
USUARIOS_INICIALES: Dict[str, Dict[str, Any]] = {
    "sede_sur": {
        "password_plano": "Sur#2026",
        "rol": Rol.SEDE,
        "sede_nombre": "Sede Sur",
        "permisos": list(PERMISOS_POR_ROL[Rol.SEDE]),
    },
    "sede_norte": {
        "password_plano": "Norte#2026",
        "rol": Rol.SEDE,
        "sede_nombre": "Sede Norte",
        "permisos": list(PERMISOS_POR_ROL[Rol.SEDE]),
    },
    "simulador": {
        "password_plano": "Sim#2026",
        "rol": Rol.SIMULADOR,
        "sede_nombre": "Simulador",
        "permisos": list(PERMISOS_POR_ROL[Rol.SIMULADOR]),
    },
}

# Alias retrocompatible para el diccionario de roles
USUARIOS_ROLES = USUARIOS_INICIALES
