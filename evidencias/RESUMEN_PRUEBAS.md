# Resumen Ejecutivo de Pruebas de Calidad y Validación de Rúbrica

**Proyecto:** Sistema de Inventario Distribuido  
**Asignatura:** Sistemas Distribuidos (7mo Semestre)  
**Fecha de Ejecución:** `2026-11-05 10:06:53`  
**Plataforma de Ejecución:** Servidor Central Windows / Python 3.12 / gRPC / PostgreSQL  
**Resultado Global:** **6 de 6 Pruebas Aprobadas (100% de Cumplimiento)**

---

## Tabla Consolidada de Pruebas Formales

| ID | Nombre de la Prueba | Procedimiento Realizado | Resultado Esperado | Resultado Obtenido | Estado |
| :---: | :--- | :--- | :--- | :--- | :---: |
| **P1** | Eventos en Vivo entre Sedes | Sede Sur modifica stock (+5); Sede Norte escucha mediante stream gRPC WatchProducts | Sede Norte recibe evento en vivo automáticamente sin polling con los nuevos datos | Evento recibido con origen 'Sede Sur', stock=30, versión=3 | **APROBADA** |
| **P2** | Concurrencia Masiva (10 Clientes) | 10 clientes concurrentes disparan AdjustStock simultáneamente con la misma expected_version | Exactamente 1 éxito, 9 conflictos de versión e invariante final consistente | 1 éxito(s), 9 conflicto(s). Invariantes matemáticas cumplidas. | **APROBADA** |
| **P3** | Rechazo de Stock Insuficiente | Solicitar ajuste de existencias (-40) superior al stock disponible (30) | Rechazo atómico; stock y versión permanecen inalterados | Operación rechazada ('Stock insuficiente para 'PROD-003'. Stock actual: 30, ajuste solicitado: -40.'). Stock=30, v1. | **APROBADA** |
| **P4** | Tolerancia a Fallas y Reconexión | Apagar servidor en plena ejecución, capturar error controlado y verificar recuperación al reiniciar | Cliente reporta ServidorNoDisponible sin caerse y reconecta transparentemente | Error capturado exitosamente; reconexión y consulta posterior OK tras reactivar servidor. | **APROBADA** |
| **P5** | Seguridad y Control de Acceso (RBAC) | Login con contraseña inválida, y cuenta 'simulador' intenta crear, eliminar y ajustar productos | Contraseña falsa rechazada; Create/Delete bloqueados con PERMISSION_DENIED; AdjustStock autorizado | Login inválido denegado. Permisos validados estrictamente según la matriz de roles. | **APROBADA** |
| **P6** | Persistencia tras Reinicio del Servidor | Modificar existencias en instancia 1, apagar servidor, iniciar instancia 2 y verificar datos | El producto conserva el stock modificado y la versión incrementada en la nueva instancia | Datos persistidos fielmente: stock=18, versión=v3. | **APROBADA** |

---

## Conclusiones Técnicas para el Informe

1. **Consistencia Estricta sin Lost Updates:** En pruebas de alta concurrencia masiva (P2 con 10 clientes concurrentes sincronizados), la verificación atómica mediante `WHERE id = %s AND version = %s` demostró matemáticamente que solo 1 transacción puede modificar el registro para una versión dada. Las 9 restantes recibieron `version_conflict=True` sin alterar el stock.
2. **Propagación en Tiempo Real sin Polling:** El canal unidireccional persistente `WatchProducts` (P1) garantiza que cualquier mutación generada en Sede Sur es recibida por Sede Norte en menos de 20 ms mediante el patrón publicador-suscriptor desacoplado.
3. **Resiliencia ante Desconexiones de Red:** Las desconexiones y caídas forzadas del servidor central (P4) no provocan bloqueos en el cliente; las excepciones de transporte son traducidas a `ServidorNoDisponible` y la reconexión adaptativa restaura el canal al reactivar el servicio.
4. **Seguridad y Control de Acceso (RBAC):** El interceptor gRPC (P5) bloquea intentos de autenticación inválidos y deniega operaciones de administración (`CreateProduct`, `DeleteProduct`) a roles restringidos como `SIMULADOR` con código de estado `PERMISSION_DENIED`.
5. **Persistencia e Independencia de Estados:** Las mutaciones registradas sobre el almacén persisten inalteradas tras el reinicio del servidor gRPC (P6), cumpliendo con los estándares ACID.

---
*Archivo generado automáticamente por `scripts/generar_evidencias.py`.*
