"""
Simulador de concurrencia distribuida para InventarioDistribuido.
Simula múltiples sedes/clientes enviando peticiones simultáneas mediante gRPC
para comprobar el control de concurrencia optimista y la consistencia del stock.
"""

import argparse
import sys
import threading
import time
from typing import List, Dict, Any
import grpc

from src.generated import inventory_pb2, inventory_pb2_grpc


def _hilo_cliente(
    sede_id: str,
    host: str,
    port: int,
    product_id: str,
    delta: int,
    expected_version: int,
    barrera: threading.Barrier,
    resultados: List[Dict[str, Any]],
    lock_resultados: threading.Lock,
):
    """Función ejecutada por cada hilo que simula una sede independiente."""
    canal = grpc.insecure_channel(f"{host}:{port}")
    stub = inventory_pb2_grpc.InventoryServiceStub(canal)

    # Esperar a que todos los hilos estén listos para disparar simultáneamente
    try:
        barrera.wait(timeout=5.0)
    except threading.BrokenBarrierError:
        pass

    resultado = {
        "sede": sede_id,
        "success": False,
        "version_conflict": False,
        "insufficient_stock": False,
        "error_message": "",
        "nuevo_stock": None,
        "nueva_version": None,
    }

    try:
        req = inventory_pb2.AdjustStockRequest(
            product_id=product_id,
            delta=delta,
            expected_version=expected_version,
        )
        resp = stub.AdjustStock(req, timeout=5.0)
        resultado["success"] = resp.success
        resultado["version_conflict"] = resp.version_conflict
        resultado["error_message"] = resp.error_message

        if not resp.success and "insuficiente" in resp.error_message.lower():
            resultado["insufficient_stock"] = True

        if resp.success and resp.HasField("product"):
            resultado["nuevo_stock"] = resp.product.stock
            resultado["nueva_version"] = resp.product.version
    except grpc.RpcError as rpc_err:
        resultado["error_message"] = f"Error gRPC ({rpc_err.code()}): {rpc_err.details()}"
    except Exception as e:
        resultado["error_message"] = f"Excepción local: {e}"
    finally:
        canal.close()
        with lock_resultados:
            resultados.append(resultado)


def ejecutar_simulacion(
    host: str = "localhost",
    port: int = 50051,
    num_clientes: int = 5,
    product_id: str = "PROD-004",
    delta: int = 2,
) -> bool:
    """
    Ejecuta la prueba de concurrencia contra el servidor central gRPC real.

    Escenario:
    1. Consulta el estado inicial del producto por gRPC.
    2. Lanza N hilos concurrentes que intentan modificar el producto usando la versión inicial compartida.
    3. Verifica que exactamente 1 operación sea aceptada y N-1 sean rechazadas por conflicto de versión.
    4. Consulta el estado final por gRPC y valida la consistencia matemática del stock y versión.
    """
    print("\n" + "=" * 70)
    print("   SIMULADOR DE SOLICITUDES CONCURRENTES - INVENTARIO DISTRIBUIDO")
    print("=" * 70)
    print(f"Servidor destino:      {host}:{port}")
    print(f"Número de sedes/hilos: {num_clientes}")
    print(f"Producto bajo prueba:  {product_id}")
    print(f"Ajuste por solicitud:  {delta:+d} unidades")

    # 1. Consulta previa por gRPC
    canal_admin = grpc.insecure_channel(f"{host}:{port}")
    stub_admin = inventory_pb2_grpc.InventoryServiceStub(canal_admin)

    try:
        resp_inicial = stub_admin.GetProduct(
            inventory_pb2.GetProductRequest(product_id=product_id), timeout=5.0
        )
    except grpc.RpcError as e:
        print(f"\n[ERROR] No se pudo conectar al servidor gRPC: {e.details() or e.code()}")
        canal_admin.close()
        return False

    if not resp_inicial.success:
        print(f"\n[ERROR] No se encontró el producto de prueba: {resp_inicial.error_message}")
        canal_admin.close()
        return False

    prod_ini = resp_inicial.product
    stock_inicial = prod_ini.stock
    version_inicial = prod_ini.version

    print(f"\n[ESTADO INICIAL REGISTRADO VIA gRPC]")
    print(f"  Producto: {prod_ini.name}")
    print(f"  Stock inicial:   {stock_inicial} unidades")
    print(f"  Versión inicial: {version_inicial}")
    print(f"\nDisparando {num_clientes} solicitudes concurrentes con expected_version={version_inicial}...")

    # 2. Configurar barrera y lanzar hilos
    barrera = threading.Barrier(num_clientes)
    resultados: List[Dict[str, Any]] = []
    lock_resultados = threading.Lock()
    hilos = []

    for i in range(1, num_clientes + 1):
        nombre_sede = f"Sede-{i:02d}"
        t = threading.Thread(
            target=_hilo_cliente,
            args=(
                nombre_sede,
                host,
                port,
                product_id,
                delta,
                version_inicial,
                barrera,
                resultados,
                lock_resultados,
            ),
        )
        hilos.append(t)
        t.start()

    # Esperar a que finalicen todos los hilos
    for t in hilos:
        t.join()

    # 3. Procesar y categorizar resultados
    exitosos = [r for r in resultados if r["success"]]
    conflictos = [r for r in resultados if r["version_conflict"]]
    stock_insuf = [r for r in resultados if r["insufficient_stock"]]
    otros_errores = [
        r for r in resultados if not r["success"] and not r["version_conflict"] and not r["insufficient_stock"]
    ]

    print("\n--- Desglose de respuestas recibidas ---")
    for r in sorted(resultados, key=lambda x: x["sede"]):
        if r["success"]:
            print(f"  [{r['sede']}] -> ÉXITO: Stock actualizado a {r['nuevo_stock']}, Versión {r['nueva_version']}")
        elif r["version_conflict"]:
            print(f"  [{r['sede']}] -> CONFLICTO DE VERSIÓN: {r['error_message']}")
        elif r["insufficient_stock"]:
            print(f"  [{r['sede']}] -> STOCK INSUFICIENTE: {r['error_message']}")
        else:
            print(f"  [{r['sede']}] -> ERROR: {r['error_message']}")

    # 4. Consulta final por gRPC
    resp_final = stub_admin.GetProduct(
        inventory_pb2.GetProductRequest(product_id=product_id), timeout=5.0
    )
    canal_admin.close()

    if not resp_final.success:
        print("\n[ERROR CRÍTICO] No se pudo obtener el estado final del producto.")
        return False

    prod_fin = resp_final.product
    stock_final = prod_fin.stock
    version_final = prod_fin.version

    stock_esperado = stock_inicial + (delta * len(exitosos))
    version_esperada = version_inicial + len(exitosos)

    print("\n" + "=" * 70)
    print("   RESUMEN Y EVALUACIÓN DE CONSISTENCIA")
    print("=" * 70)
    print(f"Total solicitudes enviadas:          {num_clientes}")
    print(f"Operaciones aceptadas (éxito):       {len(exitosos)}")
    print(f"Rechazadas por conflicto de versión: {len(conflictos)}")
    print(f"Rechazadas por stock insuficiente:   {len(stock_insuf)}")
    print(f"Otros errores:                       {len(otros_errores)}")
    print("-" * 70)
    print(f"Stock inicial:   {stock_inicial:<6} | Stock final obtenido:   {stock_final} (Esperado: {stock_esperado})")
    print(f"Versión inicial: {version_inicial:<6} | Versión final obtenida: {version_final} (Esperada: {version_esperada})")
    print("-" * 70)

    # Validaciones de consistencia
    condicion_exito_unico = len(exitosos) == 1
    condicion_conflictos = len(conflictos) == (num_clientes - 1)
    condicion_stock = stock_final == stock_esperado
    condicion_version = version_final == version_esperada

    if condicion_exito_unico and condicion_conflictos and condicion_stock and condicion_version:
        print("[PRUEBA SATISFACTORIA]")
        print("[OK] El control de concurrencia optimista funcionó con total precisión.")
        print("[OK] Solo la primera solicitud en adquirir el candado atómico fue aceptada.")
        print(f"[OK] Las {len(conflictos)} solicitudes restantes con versión desactualizada fueron rechazadas sin alterar el stock.")
        print("[OK] No hubo condición de carrera ni pérdida de datos (Lost Update).")
        return True
    else:
        print("[FALLO DE CONSISTENCIA]")
        if not condicion_exito_unico:
            print(f"[FALLO] Se esperaba exactamente 1 operación exitosa, se obtuvieron {len(exitosos)}.")
        if not condicion_conflictos:
            print(f"[FALLO] Se esperaban {num_clientes - 1} conflictos, se obtuvieron {len(conflictos)}.")
        if not condicion_stock:
            print(f"[FALLO] El stock final ({stock_final}) no coincide con el esperado ({stock_esperado}).")
        if not condicion_version:
            print(f"[FALLO] La versión final ({version_final}) no coincide con la esperada ({version_esperada}).")
        return False



def main():
    parser = argparse.ArgumentParser(description="Simulador de concurrencia para InventarioDistribuido")
    parser.add_argument("--host", default="localhost", help="Host del servidor gRPC (default: localhost)")
    parser.add_argument("--port", type=int, default=50051, help="Puerto TCP del servidor (default: 50051)")
    parser.add_argument("--clients", "-n", type=int, default=5, help="Número de clientes concurrentes (default: 5)")
    parser.add_argument("--product-id", default="PROD-004", help="ID del producto para la prueba (default: PROD-004)")
    parser.add_argument("--delta", type=int, default=2, help="Ajuste de stock por cliente (default: 2)")

    args = parser.parse_args()

    exito = ejecutar_simulacion(
        host=args.host,
        port=args.port,
        num_clientes=args.clients,
        product_id=args.product_id,
        delta=args.delta,
    )

    sys.exit(0 if exito else 1)


if __name__ == "__main__":
    main()
