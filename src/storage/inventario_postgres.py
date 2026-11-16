"""
Capa de persistencia en PostgreSQL para InventarioDistribuido.

Implementa la gestión de inventario, auditoría de movimientos y autenticación
utilizando psycopg 3 con un pool de conexiones (ConnectionPool) y control de
concurrencia optimista a nivel de fila directamente en sentencias SQL.
"""

from typing import List, Optional, Tuple, Dict, Any
import bcrypt
from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row

from src.storage.inventario_memoria import Producto
from src.config import (
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_USER,
    DB_PASSWORD,
    DB_POOL_MIN_SIZE,
    DB_POOL_MAX_SIZE,
)


class InventarioPostgres:
    """
    Almacén de inventario persistente respaldado por PostgreSQL.
    Seguro para concurrencia mediante transacciones SQL y bloqueo a nivel de fila.
    """

    def __init__(
        self,
        host: str = DB_HOST,
        port: int = DB_PORT,
        dbname: str = DB_NAME,
        user: str = DB_USER,
        password: str = DB_PASSWORD,
        min_size: int = DB_POOL_MIN_SIZE,
        max_size: int = DB_POOL_MAX_SIZE,
    ):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.min_size = min_size
        self.max_size = max_size

        conninfo = (
            f"host={self.host} port={self.port} dbname={self.dbname} "
            f"user={self.user} password={self.password}"
        )
        # Pool de conexiones psycopg 3 para atender múltiples llamadas gRPC simultáneas
        self.pool = ConnectionPool(
            conninfo=conninfo,
            min_size=self.min_size,
            max_size=self.max_size,
            open=True,
            kwargs={"autocommit": True},
        )

    def close(self):
        """Cierra de forma ordenada el pool de conexiones."""
        if self.pool and not self.pool.closed:
            self.pool.close()

    def get_product(self, product_id: str) -> Optional[Producto]:
        """Consulta un producto por su identificador único."""
        if not product_id or not isinstance(product_id, str):
            return None

        clean_id = product_id.strip()
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, nombre, stock, version FROM productos WHERE id = %s;",
                    (clean_id,),
                )
                row = cur.fetchone()
                if row:
                    return Producto(id=row[0], name=row[1], stock=row[2], version=row[3])
                return None

    def list_products(self) -> List[Producto]:
        """Retorna la lista de todos los productos ordenados por ID."""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, nombre, stock, version FROM productos ORDER BY id ASC;")
                rows = cur.fetchall()
                return [Producto(id=r[0], name=r[1], stock=r[2], version=r[3]) for r in rows]

    def create_product(self, product_id: str, name: str, stock: int) -> Tuple[bool, str, Optional[Producto]]:
        """
        Crea un nuevo producto en el catálogo.

        Retorna:
            (success, error_message, producto)
        """
        if not product_id or not isinstance(product_id, str):
            return False, "El ID del producto no puede estar vacío.", None
        clean_id = product_id.strip()

        if not name or not isinstance(name, str) or not name.strip():
            return False, "El nombre del producto no puede estar vacío.", None
        clean_name = name.strip()

        if stock < 0:
            return False, "El stock inicial no puede ser negativo.", None

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                # Comprobar si ya existe
                cur.execute("SELECT 1 FROM productos WHERE id = %s;", (clean_id,))
                if cur.fetchone():
                    return False, f"Ya existe un producto con el ID '{clean_id}'.", None

                cur.execute(
                    """
                    INSERT INTO productos (id, nombre, stock, version)
                    VALUES (%s, %s, %s, 1)
                    RETURNING id, nombre, stock, version;
                    """,
                    (clean_id, clean_name, stock),
                )
                r = cur.fetchone()
                prod = Producto(id=r[0], name=r[1], stock=r[2], version=r[3])
                return True, "", prod

    def update_product(
        self, product_id: str, name: str, expected_version: int
    ) -> Tuple[bool, str, bool, Optional[Producto]]:
        """
        Actualiza el nombre de un producto con control de concurrencia optimista.

        Retorna:
            (success, error_message, version_conflict, producto)
        """
        if not product_id or not isinstance(product_id, str):
            return False, "ID de producto inválido.", False, None
        clean_id = product_id.strip()

        if not name or not isinstance(name, str) or not name.strip():
            return False, "El nombre no puede estar vacío.", False, None
        clean_name = name.strip()

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                # Actualización atómica en SQL
                cur.execute(
                    """
                    UPDATE productos
                    SET nombre = %s, version = version + 1
                    WHERE id = %s AND version = %s
                    RETURNING id, nombre, stock, version;
                    """,
                    (clean_name, clean_id, expected_version),
                )
                row = cur.fetchone()
                if row:
                    return True, "", False, Producto(id=row[0], name=row[1], stock=row[2], version=row[3])

                # Si no se actualizó, diagnosticar la causa exacta
                cur.execute("SELECT id, nombre, stock, version FROM productos WHERE id = %s;", (clean_id,))
                actual = cur.fetchone()
                if not actual:
                    return False, f"Producto '{clean_id}' no encontrado.", False, None

                prod_actual = Producto(id=actual[0], name=actual[1], stock=actual[2], version=actual[3])
                return (
                    False,
                    f"Conflicto de versión en '{clean_id}'. Esperada: {expected_version}, actual: {prod_actual.version}.",
                    True,
                    prod_actual,
                )

    def delete_product(self, product_id: str, expected_version: int) -> Tuple[bool, str, bool]:
        """
        Elimina un producto verificando la versión esperada.

        Retorna:
            (success, error_message, version_conflict)
        """
        if not product_id or not isinstance(product_id, str):
            return False, "ID de producto inválido.", False
        clean_id = product_id.strip()

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM productos WHERE id = %s AND version = %s RETURNING id;",
                    (clean_id, expected_version),
                )
                if cur.fetchone():
                    return True, "", False

                # Diagnosticar si no existía o fue un conflicto de versión
                cur.execute("SELECT version FROM productos WHERE id = %s;", (clean_id,))
                row = cur.fetchone()
                if not row:
                    return False, f"Producto '{clean_id}' no encontrado.", False

                return (
                    False,
                    f"Conflicto de versión al eliminar '{clean_id}'. Esperada: {expected_version}, actual: {row[0]}.",
                    True,
                )

    def adjust_stock(
        self, product_id: str, delta: int, expected_version: int
    ) -> Tuple[bool, str, bool, Optional[Producto]]:
        """
        Aumenta o disminuye el stock de forma atómica dentro de la sentencia SQL.

        ¿POR QUÉ ESTO EVITA LOST UPDATES INCLUSO CON VARIOS PROCESOS O MÁQUINAS?:
        En PostgreSQL (aislamiento Read Committed por defecto), una sentencia UPDATE
        adquiere un bloqueo exclusivo a nivel de fila (Row-Exclusive Lock).
        Al incluir la condición de versión y el límite de stock directamente en la
        cláusula WHERE ('WHERE id = %s AND version = %s AND stock + %s >= 0'):
        1. El motor de PostgreSQL evalúa la condición WHERE sobre la versión física
           más reciente y confirmada del registro.
        2. Si dos clientes (en diferentes procesos o sedes remotas) envían un ajuste
           simultáneo leyendo la misma versión inicial (ej. v1):
           - El primer cliente bloquea la fila, verifica version == 1, actualiza stock,
             incrementa a v2 y confirma (commit).
           - El segundo cliente queda encolado esperando el bloqueo de la fila. Al
             desbloquearse, PostgreSQL re-evalúa el WHERE con la fila ya modificada (v2).
             Al no coincidir version = 1, el WHERE evalúa como falso.
        3. En consecuencia, la segunda transacción actualiza exactamente 0 filas sin
           sobreescribir el cambio previo, previniendo actualizaciones perdidas.
        4. La consulta de diagnóstico posterior detecta el conflicto y devuelve el
           estado actual al cliente para que pueda reintentar.

        Retorna:
            (success, error_message, version_conflict, producto)
        """
        if not product_id or not isinstance(product_id, str):
            return False, "Identificador de producto inválido o vacío.", False, None
        clean_id = product_id.strip()

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                # Actualización atómica en una sola instrucción
                cur.execute(
                    """
                    UPDATE productos
                    SET stock = stock + %s, version = version + 1
                    WHERE id = %s AND version = %s AND stock + %s >= 0
                    RETURNING id, nombre, stock, version;
                    """,
                    (delta, clean_id, expected_version, delta),
                )
                row = cur.fetchone()
                if row:
                    prod = Producto(id=row[0], name=row[1], stock=row[2], version=row[3])
                    return True, "", False, prod

                # Diagnosticar por qué no se actualizó ninguna fila
                cur.execute("SELECT id, nombre, stock, version FROM productos WHERE id = %s;", (clean_id,))
                actual = cur.fetchone()
                if not actual:
                    return False, f"Producto '{clean_id}' no encontrado.", False, None

                prod_actual = Producto(id=actual[0], name=actual[1], stock=actual[2], version=actual[3])

                if prod_actual.version != expected_version:
                    return (
                        False,
                        f"Conflicto de versión en '{clean_id}'. Esperada: {expected_version}, actual en servidor: {prod_actual.version}.",
                        True,
                        prod_actual,
                    )

                if prod_actual.stock + delta < 0:
                    return (
                        False,
                        f"Stock insuficiente para '{clean_id}'. Stock actual: {prod_actual.stock}, ajuste solicitado: {delta}.",
                        False,
                        prod_actual,
                    )

                return False, "No se pudo realizar el ajuste de existencias.", False, prod_actual

    def set_stock(
        self, product_id: str, new_stock: int, expected_version: int
    ) -> Tuple[bool, str, bool, Optional[Producto]]:
        """
        Establece una cantidad específica de existencias de forma atómica en SQL.

        Retorna:
            (success, error_message, version_conflict, producto)
        """
        if not product_id or not isinstance(product_id, str):
            return False, "Identificador de producto inválido o vacío.", False, None
        clean_id = product_id.strip()

        if new_stock < 0:
            return False, "El stock no puede ser un valor negativo.", False, None

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE productos
                    SET stock = %s, version = version + 1
                    WHERE id = %s AND version = %s
                    RETURNING id, nombre, stock, version;
                    """,
                    (new_stock, clean_id, expected_version),
                )
                row = cur.fetchone()
                if row:
                    prod = Producto(id=row[0], name=row[1], stock=row[2], version=row[3])
                    return True, "", False, prod

                # Diagnosticar causa
                cur.execute("SELECT id, nombre, stock, version FROM productos WHERE id = %s;", (clean_id,))
                actual = cur.fetchone()
                if not actual:
                    return False, f"Producto '{clean_id}' no encontrado.", False, None

                prod_actual = Producto(id=actual[0], name=actual[1], stock=actual[2], version=actual[3])
                return (
                    False,
                    f"Conflicto de versión en '{clean_id}'. Esperada: {expected_version}, actual en servidor: {prod_actual.version}.",
                    True,
                    prod_actual,
                )

    def registrar_movimiento(
        self,
        usuario: str,
        sede: str,
        operacion: str,
        producto_id: str,
        detalle: str,
        exito: bool,
        mensaje: str = "",
    ) -> None:
        """
        Registra una operación en la tabla de auditoría inmutable de movimientos.
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO movimientos (usuario, sede, operacion, producto_id, detalle, exito, mensaje)
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                    """,
                    (usuario, sede, operacion, producto_id, detalle, exito, mensaje),
                )

    def listar_movimientos(self, producto_id: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Consulta el historial de auditoría de movimientos.
        Opcionalmente filtra por producto y limita la cantidad de registros.
        """
        with self.pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                if producto_id:
                    cur.execute(
                        """
                        SELECT id, fecha, usuario, sede, operacion, producto_id, detalle, exito, mensaje
                        FROM movimientos
                        WHERE producto_id = %s
                        ORDER BY fecha DESC, id DESC
                        LIMIT %s;
                        """,
                        (producto_id.strip(), limit),
                    )
                else:
                    cur.execute(
                        """
                        SELECT id, fecha, usuario, sede, operacion, producto_id, detalle, exito, mensaje
                        FROM movimientos
                        ORDER BY fecha DESC, id DESC
                        LIMIT %s;
                        """,
                        (limit,),
                    )
                return cur.fetchall()

    def autenticar_usuario(self, username: str, password_plano: str) -> Optional[Dict[str, Any]]:
        """
        Valida las credenciales contra la tabla de usuarios utilizando bcrypt.
        Retorna la información del usuario autenticado o None si fallan las credenciales.
        """
        if not username or not password_plano:
            return None

        clean_user = username.strip()
        with self.pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    "SELECT id, username, password_hash, rol, sede_nombre FROM usuarios WHERE username = %s;",
                    (clean_user,),
                )
                row = cur.fetchone()
                if not row:
                    return None

                # Verificación criptográfica con bcrypt
                pw_bytes = password_plano.encode("utf-8")
                hash_bytes = row["password_hash"].encode("utf-8")
                if bcrypt.checkpw(pw_bytes, hash_bytes):
                    return {
                        "id": row["id"],
                        "username": row["username"],
                        "rol": row["rol"],
                        "sede_nombre": row["sede_nombre"],
                    }
                return None

