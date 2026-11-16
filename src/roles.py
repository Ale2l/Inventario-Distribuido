"""
Definición de roles y permisos para el sistema de InventarioDistribuido.

En un sistema distribuido con múltiples sedes y clientes remotos, el control
de acceso basado en roles (RBAC) debe validarse estrictamente en el servidor
central para garantizar que clientes modificados o no autorizados no ejecuten
operaciones indebidas sobre el inventario.
"""

from enum import Enum
from typing import Dict, Set


class Rol(str, Enum):
    """
    Enumeración de roles reconocidos en el sistema.
    Hereda de str para facilitar serialización y comparaciones.
    """
    SEDE = "SEDE"
    SIMULADOR = "SIMULADOR"


# Permisos atómicos del sistema
PERMISO_CONSULTAR = "consultar"
PERMISO_LISTAR = "listar"
PERMISO_CREAR_PRODUCTO = "crear_producto"
PERMISO_EDITAR_PRODUCTO = "editar_producto"
PERMISO_ELIMINAR_PRODUCTO = "eliminar_producto"
PERMISO_AJUSTAR_STOCK = "ajustar_stock"
PERMISO_FIJAR_STOCK = "fijar_stock"
PERMISO_VER_HISTORIAL = "ver_historial"
PERMISO_RECIBIR_EVENTOS = "recibir_eventos"

# Matriz de permisos por rol según la especificación del proyecto
PERMISOS_POR_ROL: Dict[Rol, Set[str]] = {
    Rol.SEDE: {
        PERMISO_CONSULTAR,
        PERMISO_LISTAR,
        PERMISO_CREAR_PRODUCTO,
        PERMISO_EDITAR_PRODUCTO,
        PERMISO_ELIMINAR_PRODUCTO,
        PERMISO_AJUSTAR_STOCK,
        PERMISO_FIJAR_STOCK,
        PERMISO_VER_HISTORIAL,
        PERMISO_RECIBIR_EVENTOS,
    },
    Rol.SIMULADOR: {
        PERMISO_CONSULTAR,
        PERMISO_LISTAR,
        PERMISO_AJUSTAR_STOCK,
        PERMISO_RECIBIR_EVENTOS,
    },
}


def tiene_permiso(rol: Rol | str, permiso: str) -> bool:
    """
    Verifica si un rol posee autorización para ejecutar una acción específica.

    ¿Por qué funciona así?:
    En arquitecturas cliente-servidor distribuidas, el principio de menor
    privilegio exige que cada invocación a un RPC sea contrastada contra los
    permisos del rol autenticado antes de acceder a la capa de persistencia.
    """
    try:
        rol_enum = Rol(rol) if isinstance(rol, str) else rol
    except ValueError:
        return False

    permisos_asignados = PERMISOS_POR_ROL.get(rol_enum, set())
    return permiso in permisos_asignados

