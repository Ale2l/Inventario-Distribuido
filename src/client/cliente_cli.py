"""
Cliente interactivo de consola para InventarioDistribuido.
Permite a los operadores de distintas sedes consultar y modificar el inventario mediante gRPC.
"""

import sys
import grpc
from typing import Optional

from src.generated import inventory_pb2, inventory_pb2_grpc


class ClienteInventario:
    """Encapsula la comunicación gRPC con el servidor central de inventario."""

    def __init__(self, host: str = "localhost", port: int = 50051, sede: str = "Sede Principal", timeout: float = 5.0):
        self.host = host
        self.port = port
        self.sede = sede
        self.timeout = timeout
        self.target = f"{host}:{port}"
        self.canal = grpc.insecure_channel(self.target)
        self.stub = inventory_pb2_grpc.InventoryServiceStub(self.canal)

    def cerrar(self):
        """Cierra de forma segura el canal gRPC."""
        if self.canal:
            self.canal.close()

    def listar_productos(self) -> inventory_pb2.ListProductsResponse:
        """Invoca el RPC ListProducts."""
        request = inventory_pb2.ListProductsRequest()
        return self.stub.ListProducts(request, timeout=self.timeout)

    def consultar_producto(self, product_id: str) -> inventory_pb2.GetProductResponse:
        """Invoca el RPC GetProduct."""
        request = inventory_pb2.GetProductRequest(product_id=product_id.strip())
        return self.stub.GetProduct(request, timeout=self.timeout)

    def ajustar_stock(self, product_id: str, delta: int, expected_version: int) -> inventory_pb2.AdjustStockResponse:
        """Invoca el RPC AdjustStock enviando la versión esperada."""
        request = inventory_pb2.AdjustStockRequest(
            product_id=product_id.strip(),
            delta=delta,
            expected_version=expected_version,
        )
        return self.stub.AdjustStock(request, timeout=self.timeout)

    def establecer_stock(self, product_id: str, new_stock: int, expected_version: int) -> inventory_pb2.SetStockResponse:
        """Invoca el RPC SetStock enviando la versión esperada."""
        request = inventory_pb2.SetStockRequest(
            product_id=product_id.strip(),
            new_stock=new_stock,
            expected_version=expected_version,
        )
        return self.stub.SetStock(request, timeout=self.timeout)


def _imprimir_tabla_productos(productos):
    """Muestra los productos en formato tabular legible."""
    if not productos:
        print("El catálogo está vacío actualmente.")
        return

    separador = "+" + "-" * 12 + "+" + "-" * 34 + "+" + "-" * 10 + "+" + "-" * 10 + "+"
    print(separador)
    print(f"| {'ID':<10} | {'Nombre del Producto':<32} | {'Stock':<8} | {'Versión':<8} |")
    print(separador)
    for p in productos:
        print(f"| {p.id:<10} | {p.name:<32} | {p.stock:<8} | {p.version:<8} |")
    print(separador)


def _ejecutar_con_manejo_errores(accion_fn):
    """Ejecuta una acción gRPC capturando errores de conexión, timeout y excepciones RPC."""
    try:
        return accion_fn()
    except grpc.RpcError as rpc_error:
        codigo = rpc_error.code()
        if codigo == grpc.StatusCode.UNAVAILABLE:
            print(f"\n[ERROR DE CONEXIÓN] El servidor central no está disponible en este momento.")
            print("Verifica que el servidor esté encendido ejecutando: .\\.venv\\Scripts\\python.exe -m src.server.servidor")
        elif codigo == grpc.StatusCode.DEADLINE_EXCEEDED:
            print(f"\n[ERROR DE TIEMPO DE ESPERA] La solicitud al servidor superó el tiempo límite.")
        else:
            print(f"\n[ERROR RPC] Ocurrió un error en la llamada ({codigo}): {rpc_error.details()}")
        return None
    except Exception as e:
        print(f"\n[ERROR INESPERADO] {e}")
        return None


def menu_interactivo():
    """Función de interfaz de usuario de consola interactiva."""
    print("=" * 60)
    print("   SISTEMA DE INVENTARIO DISTRIBUIDO - TERMINAL DE SEDE")
    print("=" * 60)

    try:
        nombre_sede = input("Ingrese el nombre identificador de su sede [Sede Central]: ").strip()
    except (KeyboardInterrupt, EOFError):
        print("\nOperación cancelada. Saliendo...")
        return

    if not nombre_sede:
        nombre_sede = "Sede Central"

    cliente = ClienteInventario(sede=nombre_sede)
    print(f"\n[OK] Conectado a localhost:50051 en representación de '{nombre_sede}'.")

    try:
        while True:
            print("\n" + "-" * 45)
            print(f"  MENÚ PRINCIPAL - {cliente.sede.upper()}")
            print("-" * 45)
            print("1. Listar todos los productos")
            print("2. Consultar un producto por su identificador")
            print("3. Aumentar o disminuir el stock de un producto")
            print("4. Establecer una cantidad específica de stock")
            print("5. Salir")
            print("-" * 45)

            try:
                opcion = input("Seleccione una opción (1-5): ").strip()
            except (KeyboardInterrupt, EOFError):
                print("\nInterrupción detectada. Cerrando terminal...")
                break

            if opcion == "1":
                print(f"\n--- Catálogo de Productos ({cliente.sede}) ---")
                def listar():
                    resp = cliente.listar_productos()
                    if resp.success:
                        _imprimir_tabla_productos(resp.products)
                    else:
                        print(f"[FALLO] Error del servidor: {resp.error_message}")
                _ejecutar_con_manejo_errores(listar)

            elif opcion == "2":
                product_id = input("Ingrese el ID del producto (ej. PROD-001): ").strip()
                if not product_id:
                    print("[AVISO] Debe ingresar un identificador válido.")
                    continue

                def consultar():
                    resp = cliente.consultar_producto(product_id)
                    if resp.success:
                        p = resp.product
                        print("\n--- Información del Producto ---")
                        print(f"  ID:      {p.id}")
                        print(f"  Nombre:  {p.name}")
                        print(f"  Stock:   {p.stock} unidades")
                        print(f"  Versión: {p.version}")
                    else:
                        print(f"\n[AVISO] {resp.error_message}")
                _ejecutar_con_manejo_errores(consultar)

            elif opcion == "3":
                product_id = input("Ingrese el ID del producto a modificar: ").strip()
                if not product_id:
                    print("[AVISO] El ID no puede estar vacío.")
                    continue

                # 1. Consultar primero para obtener versión actual
                def flujo_ajustar():
                    resp_get = cliente.consultar_producto(product_id)
                    if not resp_get.success:
                        print(f"\n[AVISO] {resp_get.error_message}")
                        return

                    prod_actual = resp_get.product
                    print(f"Producto encontrado: {prod_actual.name}")
                    print(f"Stock actual: {prod_actual.stock} | Versión actual: {prod_actual.version}")

                    entrada_delta = input("Ingrese el ajuste (+N para sumar, -N para restar): ").strip()
                    try:
                        delta = int(entrada_delta)
                    except ValueError:
                        print("[ERROR] El ajuste debe ser un número entero (positivo o negativo).")
                        return

                    if delta == 0:
                        print("[AVISO] El ajuste no puede ser 0.")
                        return

                    # 2. Enviar ajuste con la versión esperada
                    resp_adj = cliente.ajustar_stock(product_id, delta, expected_version=prod_actual.version)
                    if resp_adj.success:
                        p = resp_adj.product
                        print(f"\n[ÉXITO] Stock ajustado correctamente por {cliente.sede}:")
                        print(f"  Nuevo Stock:   {p.stock} (cambio: {delta:+d})")
                        print(f"  Nueva Versión: {p.version}")
                    else:
                        if resp_adj.version_conflict:
                            print(f"\n[CONFLICTO DE CONCURRENCIA] Otra sede modificó este producto simultáneamente.")
                            print(f"Detalle: {resp_adj.error_message}")
                            if resp_adj.HasField("product"):
                                print(f"Estado en el servidor: Stock={resp_adj.product.stock}, Versión={resp_adj.product.version}")
                        else:
                            print(f"\n[OPERACIÓN RECHAZADA] {resp_adj.error_message}")

                _ejecutar_con_manejo_errores(flujo_ajustar)

            elif opcion == "4":
                product_id = input("Ingrese el ID del producto a establecer stock: ").strip()
                if not product_id:
                    print("[AVISO] El ID no puede estar vacío.")
                    continue

                def flujo_establecer():
                    resp_get = cliente.consultar_producto(product_id)
                    if not resp_get.success:
                        print(f"\n[AVISO] {resp_get.error_message}")
                        return

                    prod_actual = resp_get.product
                    print(f"Producto encontrado: {prod_actual.name}")
                    print(f"Stock actual: {prod_actual.stock} | Versión actual: {prod_actual.version}")

                    entrada_stock = input("Ingrese la nueva cantidad exacta de stock: ").strip()
                    try:
                        nuevo_stock = int(entrada_stock)
                    except ValueError:
                        print("[ERROR] El stock debe ser un número entero no negativo.")
                        return

                    if nuevo_stock < 0:
                        print("[ERROR] El stock no puede ser negativo.")
                        return

                    resp_set = cliente.establecer_stock(product_id, nuevo_stock, expected_version=prod_actual.version)
                    if resp_set.success:
                        p = resp_set.product
                        print(f"\n[ÉXITO] Stock fijado correctamente por {cliente.sede}:")
                        print(f"  Nuevo Stock:   {p.stock}")
                        print(f"  Nueva Versión: {p.version}")
                    else:
                        if resp_set.version_conflict:
                            print(f"\n[CONFLICTO DE CONCURRENCIA] Otra sede modificó este producto simultáneamente.")
                            print(f"Detalle: {resp_set.error_message}")
                            if resp_set.HasField("product"):
                                print(f"Estado en el servidor: Stock={resp_set.product.stock}, Versión={resp_set.product.version}")
                        else:
                            print(f"\n[OPERACIÓN RECHAZADA] {resp_set.error_message}")

                _ejecutar_con_manejo_errores(flujo_establecer)

            elif opcion == "5":
                print(f"\nCerrando sesión de '{cliente.sede}'...")
                break

            else:
                print("[AVISO] Opción no válida. Por favor elija un número del 1 al 5.")

    finally:
        cliente.cerrar()
        print("[OK] Canal gRPC cerrado. ¡Hasta luego!\n")


if __name__ == "__main__":
    menu_interactivo()
