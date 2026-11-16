"""
Pruebas unitarias para roles, permisos y configuración centralizada.
"""

import unittest
from src.roles import (
    Rol,
    PERMISOS_POR_ROL,
    tiene_permiso,
    PERMISO_CONSULTAR,
    PERMISO_LISTAR,
    PERMISO_CREAR_PRODUCTO,
    PERMISO_EDITAR_PRODUCTO,
    PERMISO_ELIMINAR_PRODUCTO,
    PERMISO_AJUSTAR_STOCK,
    PERMISO_FIJAR_STOCK,
    PERMISO_VER_HISTORIAL,
    PERMISO_RECIBIR_EVENTOS,
)
from src.config import (
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_USER,
    DB_PASSWORD,
    DB_TEST_NAME,
    GRPC_SERVER_HOST,
    GRPC_SERVER_PORT,
    GRPC_TIMEOUT,
    USUARIOS_INICIALES,
)


class TestRolesYConfig(unittest.TestCase):
    """Verifica la definición de roles, matriz de permisos y configuración embebida."""

    def test_roles_enumeracion(self):
        """Verifica que existan exactamente los roles SEDE y SIMULADOR."""
        self.assertEqual(Rol.SEDE.value, "SEDE")
        self.assertEqual(Rol.SIMULADOR.value, "SIMULADOR")

    def test_permisos_sede(self):
        """Verifica que el rol SEDE tenga todos los permisos requeridos."""
        permisos_esperados = {
            PERMISO_CONSULTAR,
            PERMISO_LISTAR,
            PERMISO_CREAR_PRODUCTO,
            PERMISO_EDITAR_PRODUCTO,
            PERMISO_ELIMINAR_PRODUCTO,
            PERMISO_AJUSTAR_STOCK,
            PERMISO_FIJAR_STOCK,
            PERMISO_VER_HISTORIAL,
            PERMISO_RECIBIR_EVENTOS,
        }
        self.assertEqual(PERMISOS_POR_ROL[Rol.SEDE], permisos_esperados)
        for p in permisos_esperados:
            self.assertTrue(tiene_permiso(Rol.SEDE, p))
            self.assertTrue(tiene_permiso("SEDE", p))

    def test_permisos_simulador(self):
        """Verifica que el rol SIMULADOR tenga únicamente los permisos autorizados."""
        permisos_esperados = {
            PERMISO_CONSULTAR,
            PERMISO_LISTAR,
            PERMISO_AJUSTAR_STOCK,
            PERMISO_RECIBIR_EVENTOS,
        }
        self.assertEqual(PERMISOS_POR_ROL[Rol.SIMULADOR], permisos_esperados)
        for p in permisos_esperados:
            self.assertTrue(tiene_permiso(Rol.SIMULADOR, p))

        # El simulador NO debe tener permisos de modificación estructural
        self.assertFalse(tiene_permiso(Rol.SIMULADOR, PERMISO_CREAR_PRODUCTO))
        self.assertFalse(tiene_permiso(Rol.SIMULADOR, PERMISO_EDITAR_PRODUCTO))
        self.assertFalse(tiene_permiso(Rol.SIMULADOR, PERMISO_ELIMINAR_PRODUCTO))
        self.assertFalse(tiene_permiso(Rol.SIMULADOR, PERMISO_FIJAR_STOCK))
        self.assertFalse(tiene_permiso(Rol.SIMULADOR, PERMISO_VER_HISTORIAL))

    def test_tiene_permiso_rol_invalido(self):
        """Verifica que roles inexistentes retornen False sin excepción."""
        self.assertFalse(tiene_permiso("ROL_INEXISTENTE", PERMISO_CONSULTAR))

    def test_configuracion_valores_por_defecto(self):
        """Verifica los valores por defecto requeridos en la especificación."""
        self.assertEqual(DB_HOST, "localhost")
        self.assertEqual(DB_PORT, 5432)
        self.assertEqual(DB_NAME, "inventario_distribuido")
        self.assertEqual(DB_USER, "inventario_app")
        self.assertEqual(DB_PASSWORD, "Inv#Dist2026")
        self.assertEqual(DB_TEST_NAME, "inventario_test")
        self.assertEqual(GRPC_SERVER_HOST, "0.0.0.0")
        self.assertEqual(GRPC_SERVER_PORT, 50051)
        self.assertEqual(GRPC_TIMEOUT, 5.0)

    def test_usuarios_iniciales_credenciales(self):
        """Verifica que existan los 3 usuarios fijos con sus roles correspondientes."""
        self.assertIn("sede_sur", USUARIOS_INICIALES)
        self.assertEqual(USUARIOS_INICIALES["sede_sur"]["password_plano"], "Sur#2026")
        self.assertEqual(USUARIOS_INICIALES["sede_sur"]["rol"], Rol.SEDE)
        self.assertEqual(USUARIOS_INICIALES["sede_sur"]["sede_nombre"], "Sede Sur")

        self.assertIn("sede_norte", USUARIOS_INICIALES)
        self.assertEqual(USUARIOS_INICIALES["sede_norte"]["password_plano"], "Norte#2026")
        self.assertEqual(USUARIOS_INICIALES["sede_norte"]["rol"], Rol.SEDE)
        self.assertEqual(USUARIOS_INICIALES["sede_norte"]["sede_nombre"], "Sede Norte")

        self.assertIn("simulador", USUARIOS_INICIALES)
        self.assertEqual(USUARIOS_INICIALES["simulador"]["password_plano"], "Sim#2026")
        self.assertEqual(USUARIOS_INICIALES["simulador"]["rol"], Rol.SIMULADOR)
        self.assertEqual(USUARIOS_INICIALES["simulador"]["sede_nombre"], "Simulador")


if __name__ == "__main__":
    unittest.main()

