"""
Script de inicialización y migración de la base de datos PostgreSQL.

Crea el usuario inventario_app, las bases de datos inventario_distribuido e
inventario_test, las tablas necesarias y siembra los datos iniciales.
Es completamente idempotente: puede ejecutarse múltiples veces de forma segura.
"""

import getpass
import os
import sys
from pathlib import Path
from typing import Optional
import bcrypt
import psycopg
from psycopg import sql

# Agregar raíz del proyecto al sys.path para importar configuración
_raiz = str(Path(__file__).resolve().parent.parent)
if _raiz not in sys.path:
    sys.path.insert(0, _raiz)

from src.config import (
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_TEST_NAME,
    DB_USER,
    DB_PASSWORD,
    USUARIOS_INICIALES,
)

# Catálogo de productos de prueba iniciales
PRODUCTOS_INICIALES = [
    ("PROD-001", "Laptop ThinkPad T14", 15, 1),
    ("PROD-002", "Mouse Inalámbrico Logitech", 50, 1),
    ("PROD-003", "Teclado Mecánico Keychron K2", 30, 1),
    ("PROD-004", "Monitor Dell 27 Pulgadas 4K", 20, 1),
    ("PROD-005", "Auriculares Sony WH-1000XM5", 10, 1),
]

DDL_ESQUEMA = """
-- Tabla de usuarios y credenciales para autenticación en el servidor central
CREATE TABLE IF NOT EXISTS usuarios (
    id SERIAL PRIMARY KEY,
    username TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    rol TEXT NOT NULL,
    sede_nombre TEXT NOT NULL
);

-- Tabla de productos con control de concurrencia optimista por versión
CREATE TABLE IF NOT EXISTS productos (
    id TEXT PRIMARY KEY,
    nombre TEXT NOT NULL,
    stock INT NOT NULL CHECK (stock >= 0),
    version BIGINT NOT NULL DEFAULT 1
);

-- Tabla de historial de auditoría de movimientos del inventario distribuido
-- Nota de diseño: No tiene Foreign Key a 'productos' para conservar el registro
-- histórico inmutable incluso si un producto es eliminado en el futuro.
CREATE TABLE IF NOT EXISTS movimientos (
    id SERIAL PRIMARY KEY,
    fecha TIMESTAMPTZ DEFAULT NOW(),
    usuario TEXT,
    sede TEXT,
    operacion TEXT,
    producto_id TEXT,
    detalle TEXT,
    exito BOOLEAN,
    mensaje TEXT
);

-- Índices para optimizar consultas de auditoría
CREATE INDEX IF NOT EXISTS idx_movimientos_producto_id ON movimientos (producto_id);
CREATE INDEX IF NOT EXISTS idx_movimientos_fecha ON movimientos (fecha DESC);
"""


def _hashear_password(password_plano: str) -> str:
    """Genera el hash seguro bcrypt para una contraseña en texto plano."""
    salt = bcrypt.gensalt(rounds=12)
    return bcrypt.hashpw(password_plano.encode("utf-8"), salt).decode("utf-8")


def conectar_admin(password_admin: str, host: str = DB_HOST, port: int = DB_PORT):
    """Establece conexión con la base 'postgres' como superusuario."""
    return psycopg.connect(
        host=host,
        port=port,
        user="postgres",
        password=password_admin,
        dbname="postgres",
        autocommit=True,
    )


def crear_usuario_y_bases(conn_admin):
    """Crea el rol 'inventario_app' y las bases de datos si no existen."""
    with conn_admin.cursor() as cur:
        # 1. Crear o actualizar rol de aplicación usando psycopg.sql para DDL seguro
        print(f"[*] Verificando rol de base de datos '{DB_USER}'...")
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (DB_USER,))
        if not cur.fetchone():
            cur.execute(
                sql.SQL("CREATE ROLE {user} WITH LOGIN PASSWORD {pwd} CREATEDB;").format(
                    user=sql.Identifier(DB_USER),
                    pwd=sql.Literal(DB_PASSWORD),
                )
            )
            print(f"[+] Rol '{DB_USER}' creado exitosamente.")
        else:
            cur.execute(
                sql.SQL("ALTER ROLE {user} WITH LOGIN PASSWORD {pwd};").format(
                    user=sql.Identifier(DB_USER),
                    pwd=sql.Literal(DB_PASSWORD),
                )
            )
            print(f"[OK] Rol '{DB_USER}' ya existe. Contraseña sincronizada.")

        # 2. Crear bases de datos si no existen
        for nombre_bd in [DB_NAME, DB_TEST_NAME]:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (nombre_bd,))
            if not cur.fetchone():
                cur.execute(
                    sql.SQL("CREATE DATABASE {db} OWNER {user};").format(
                        db=sql.Identifier(nombre_bd),
                        user=sql.Identifier(DB_USER),
                    )
                )
                print(f"[+] Base de datos '{nombre_bd}' creada con propietario '{DB_USER}'.")
            else:
                print(f"[OK] Base de datos '{nombre_bd}' ya existe.")


def inicializar_esquema_y_datos(nombre_bd: str, host: str = DB_HOST, port: int = DB_PORT):
    """Aplica el DDL e inserta los datos iniciales de forma idempotente en la base indicada."""
    print(f"\n[*] Inicializando esquema y datos en '{nombre_bd}'...")
    conn = psycopg.connect(
        host=host,
        port=port,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=nombre_bd,
        autocommit=True,
    )

    with conn:
        with conn.cursor() as cur:
            # 1. Crear tablas
            cur.execute(DDL_ESQUEMA)
            print(f"[OK] Tablas 'usuarios', 'productos' y 'movimientos' verificadas en '{nombre_bd}'.")

            # 2. Sembrar usuarios iniciales con contraseñas bcrypt
            for username, datos in USUARIOS_INICIALES.items():
                hash_pw = _hashear_password(datos["password_plano"])
                cur.execute(
                    """
                    INSERT INTO usuarios (username, password_hash, rol, sede_nombre)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (username) DO UPDATE
                    SET password_hash = EXCLUDED.password_hash,
                        rol = EXCLUDED.rol,
                        sede_nombre = EXCLUDED.sede_nombre;
                    """,
                    (username, hash_pw, datos["rol"].value, datos["sede_nombre"]),
                )
            print(f"[OK] {len(USUARIOS_INICIALES)} cuentas de usuario sembradas/actualizadas con bcrypt.")

            # 3. Sembrar productos iniciales
            for pid, nombre, stock, version in PRODUCTOS_INICIALES:
                cur.execute(
                    """
                    INSERT INTO productos (id, nombre, stock, version)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING;
                    """,
                    (pid, nombre, stock, version),
                )
            print(f"[OK] Catálogo inicial de productos verificado en '{nombre_bd}'.")

    conn.close()


def ejecutar_migracion(password_admin: Optional[str] = None):
    """Punto de entrada principal para la migración idempotente de PostgreSQL."""
    print("=" * 65)
    print("  INICIALIZACIÓN DE POSTGRESQL - INVENTARIO DISTRIBUIDO")
    print("=" * 65)

    if not password_admin:
        # Intentar obtener de variable de entorno o solicitar por consola
        password_admin = os.getenv("POSTGRES_ADMIN_PASSWORD") or os.getenv("PGPASSWORD")
        if not password_admin:
            try:
                password_admin = getpass.getpass("Ingrese la contraseña del administrador 'postgres': ").strip()
            except (KeyboardInterrupt, EOFError):
                print("\nOperación cancelada por el usuario.")
                sys.exit(1)

    try:
        conn_admin = conectar_admin(password_admin)
        print("[+] Conexión de administración exitosa con PostgreSQL.")
    except Exception as e:
        print(f"\n[ERROR] No se pudo conectar como usuario 'postgres': {e}")
        print("Verifique que el servicio de PostgreSQL esté en ejecución y la contraseña sea correcta.")
        sys.exit(1)

    try:
        crear_usuario_y_bases(conn_admin)
    finally:
        conn_admin.close()

    # Inicializar tanto la base principal como la base de pruebas unitarias
    inicializar_esquema_y_datos(DB_NAME)
    inicializar_esquema_y_datos(DB_TEST_NAME)

    print("\n" + "=" * 65)
    print("✓ Migración completada con éxito en ambas bases de datos.")
    print("=" * 65)


if __name__ == "__main__":
    pwd_arg = sys.argv[1] if len(sys.argv) > 1 else None
    ejecutar_migracion(pwd_arg)

