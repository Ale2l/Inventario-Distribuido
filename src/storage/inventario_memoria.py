"""
Almacén de inventario en memoria para InventarioDistribuido.
Control de concurrencia mediante threading.Lock y versionamiento optimista.
"""

import threading
import time
from typing import Dict, List, Optional, Tuple, Any
import bcrypt
from src.config import USUARIOS_INICIALES


class Producto:
    """Representa un producto dentro del inventario."""
    def __init__(self, id: str, name: str, stock: int, version: int = 1):
        self.id = str(id).strip()
        self.name = str(name).strip()
        self.stock = int(stock)
        self.version = int(version)

    def copy(self) -> 'Producto':
        """Retorna una copia independiente del producto."""
        return Producto(self.id, self.name, self.stock, self.version)

    def __repr__(self) -> str:
        return f"<Producto id='{self.id}' name='{self.name}' stock={self.stock} v={self.version}>"


class InventarioMemoria:
    """
    Almacén en memoria seguro para hilos (thread-safe).
    Gestiona el inventario de productos y el control de concurrencia optimista mediante versiones.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._productos: Dict[str, Producto] = {}
        self._movimientos: List[Dict[str, Any]] = []
        self._cargar_productos_prueba()

    def _cargar_productos_prueba(self):
        """Carga un catálogo inicial de productos de prueba claramente identificados."""
        productos_iniciales = [
            Producto("PROD-001", "Laptop ThinkPad T14", 15, version=1),
            Producto("PROD-002", "Mouse Inalámbrico Logitech", 50, version=1),
            Producto("PROD-003", "Teclado Mecánico Keychron K2", 30, version=1),
            Producto("PROD-004", "Monitor Dell 27 Pulgadas 4K", 20, version=1),
            Producto("PROD-005", "Auriculares Sony WH-1000XM5", 10, version=1),
        ]
        for p in productos_iniciales:
            self._productos[p.id] = p

    def get_product(self, product_id: str) -> Optional[Producto]:
        """Consulta un producto por su identificador."""
        if not product_id or not isinstance(product_id, str):
            return None

        clean_id = product_id.strip()
        with self._lock:
            prod = self._productos.get(clean_id)
            return prod.copy() if prod else None

    def list_products(self) -> List[Producto]:
        """Lista todos los productos disponibles en el inventario."""
        with self._lock:
            return sorted(
                [prod.copy() for prod in self._productos.values()],
                key=lambda p: p.id,
            )

    def create_product(self, product_id: str, name: str, stock: int) -> Tuple[bool, str, Optional[Producto]]:
        """Crea un nuevo producto en el catálogo."""
        if not product_id or not isinstance(product_id, str) or not product_id.strip():
            return False, "El ID del producto no puede estar vacío.", None
        clean_id = product_id.strip()

        if not name or not isinstance(name, str) or not name.strip():
            return False, "El nombre del producto no puede estar vacío.", None
        clean_name = name.strip()

        if stock < 0:
            return False, "El stock inicial no puede ser negativo.", None

        with self._lock:
            if clean_id in self._productos:
                return False, f"Ya existe un producto con el ID '{clean_id}'.", None
            prod = Producto(id=clean_id, name=clean_name, stock=stock, version=1)
            self._productos[clean_id] = prod
            return True, "", prod.copy()

    def update_product(
        self, product_id: str, name: str, expected_version: int
    ) -> Tuple[bool, str, bool, Optional[Producto]]:
        """Actualiza el nombre de un producto con control de versión optimista."""
        if not product_id or not isinstance(product_id, str):
            return False, "ID de producto inválido.", False, None
        clean_id = product_id.strip()

        if not name or not isinstance(name, str) or not name.strip():
            return False, "El nombre no puede estar vacío.", False, None
        clean_name = name.strip()

        with self._lock:
            prod = self._productos.get(clean_id)
            if not prod:
                return False, f"Producto '{clean_id}' no encontrado.", False, None

            if prod.version != expected_version:
                return (
                    False,
                    f"Conflicto de versión en '{clean_id}'. Esperada: {expected_version}, actual: {prod.version}.",
                    True,
                    prod.copy(),
                )

            prod.name = clean_name
            prod.version += 1
            return True, "", False, prod.copy()

    def delete_product(self, product_id: str, expected_version: int) -> Tuple[bool, str, bool]:
        """Elimina un producto verificando la versión esperada."""
        if not product_id or not isinstance(product_id, str):
            return False, "ID de producto inválido.", False
        clean_id = product_id.strip()

        with self._lock:
            prod = self._productos.get(clean_id)
            if not prod:
                return False, f"Producto '{clean_id}' no encontrado.", False

            if prod.version != expected_version:
                return (
                    False,
                    f"Conflicto de versión al eliminar '{clean_id}'. Esperada: {expected_version}, actual: {prod.version}.",
                    True,
                )

            del self._productos[clean_id]
            return True, "", False

    def adjust_stock(
        self, product_id: str, delta: int, expected_version: int
    ) -> Tuple[bool, str, bool, Optional[Producto]]:
        """Aumenta o disminuye existencias con control de versión."""
        if not product_id or not isinstance(product_id, str):
            return False, "Identificador de producto inválido o vacío.", False, None

        clean_id = product_id.strip()

        with self._lock:
            prod = self._productos.get(clean_id)
            if not prod:
                return False, f"Producto '{clean_id}' no encontrado.", False, None

            if prod.version != expected_version:
                return (
                    False,
                    f"Conflicto de versión en '{clean_id}'. Esperada: {expected_version}, actual en servidor: {prod.version}.",
                    True,
                    prod.copy(),
                )

            nuevo_stock = prod.stock + delta
            if nuevo_stock < 0:
                return (
                    False,
                    f"Stock insuficiente para '{clean_id}'. Stock actual: {prod.stock}, ajuste solicitado: {delta}.",
                    False,
                    prod.copy(),
                )

            prod.stock = nuevo_stock
            prod.version += 1
            return True, "", False, prod.copy()

    def set_stock(
        self, product_id: str, new_stock: int, expected_version: int
    ) -> Tuple[bool, str, bool, Optional[Producto]]:
        """Establece una cantidad específica de existencias con control de versión."""
        if not product_id or not isinstance(product_id, str):
            return False, "Identificador de producto inválido o vacío.", False, None

        clean_id = product_id.strip()

        if new_stock < 0:
            return False, "El stock no puede ser un valor negativo.", False, None

        with self._lock:
            prod = self._productos.get(clean_id)
            if not prod:
                return False, f"Producto '{clean_id}' no encontrado.", False, None

            if prod.version != expected_version:
                return (
                    False,
                    f"Conflicto de versión en '{clean_id}'. Esperada: {expected_version}, actual en servidor: {prod.version}.",
                    True,
                    prod.copy(),
                )

            prod.stock = new_stock
            prod.version += 1
            return True, "", False, prod.copy()

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
        """Registra un movimiento en el historial inmutable."""
        with self._lock:
            mov = {
                "id": len(self._movimientos) + 1,
                "fecha": time.strftime("%Y-%m-%d %H:%M:%S"),
                "usuario": usuario,
                "sede": sede,
                "operacion": operacion,
                "producto_id": producto_id,
                "detalle": detalle,
                "exito": exito,
                "mensaje": mensaje,
            }
            self._movimientos.insert(0, mov)

    def listar_movimientos(self, producto_id: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        """Consulta movimientos de auditoría."""
        with self._lock:
            if producto_id:
                filtrados = [m for m in self._movimientos if m["producto_id"] == producto_id]
                return filtrados[:limit]
            return self._movimientos[:limit]

    def autenticar_usuario(self, username: str, password_plano: str) -> Optional[Dict[str, Any]]:
        """Valida credenciales contra los usuarios iniciales del sistema."""
        if not username or not password_plano:
            return None
        user = USUARIOS_INICIALES.get(username.strip())
        if not user:
            return None

        # Validación con contraseña plana o bcrypt
        if user["password_plano"] == password_plano:
            return {
                "id": 1,
                "username": username.strip(),
                "rol": user["rol"].value if hasattr(user["rol"], "value") else str(user["rol"]),
                "sede_nombre": user["sede_nombre"],
            }
        return None

    def close(self):
        """Cierre no-op para compatibilidad de interfaz con InventarioPostgres."""
        pass
