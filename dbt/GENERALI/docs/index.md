{% docs index %}




# 📞 Proyecto de Análisis de Llamadas

Bienvenido a la documentación oficial del pipeline de datos de llamadas 📊.  
Este proyecto tiene como objetivo centralizar, transformar y disponibilizar datos de llamadas telefónicas provenientes de diferentes fuentes para análisis operacional y estratégico.


---

## 🎯 Objetivos

- Homogeneizar y limpiar los datos de llamadas provenientes de múltiples orígenes.
- Calcular métricas clave como tiempo de espera, tiempo de llamada, abandono y atención.
- Identificar comportamientos de atención y desempeño por operador.
- Construir modelos incrementales robustos para facilitar la exploración de datos históricos.

---

## 🧱 Arquitectura del Modelo

Nuestro proyecto está organizado en tres capas siguiendo buenas prácticas de ingeniería de datos:

| Capa     | Descripción                                                                 |
|----------|------------------------------------------------------------------------------|
| **Bronze** | Ingesta directa desde la tabla cruda `DATA_WAREHAUSE_FINAL_MERGE`.        |
| **Silver** | Transformaciones limpias como `call_cleaning`, cálculo de timestamps y duración. |
| **Gold**   | Modelos listos para análisis e integración, como `call_incremental`.       |

---

## 🧪 Modelos dbt principales

| Modelo             | Tipo         | Descripción                                                                 |
|--------------------|--------------|------------------------------------------------------------------------------|
| `call_cleaning`    | Table        | Limpieza inicial, normalización de fechas y agentes.                         |
| `call_details`     | Table        | Cálculo de tiempos derivados (espera, duración, abandono, atención).         |
| `call_incremental` | Incremental  | Carga incremental de llamadas basadas en `fecha_carga` y `unique_id`.        |

---

## 🛡️ Validaciones y Tests

Se aplican pruebas automáticas en los modelos para garantizar la calidad del dato:

- `unique_id`: debe ser único y no nulo.
- `fecha_carga`: no nula, utilizada para incrementos.
- Validación de formatos de tiempo en duración (`HH:MM:SS`).



---

## 🧭 Navegación

Explora la documentación técnica detallada en las siguientes secciones:

- `call_cleaning.sql`
- `call_details.sql`
- `call_incremental.sql`
- `schema.yml`
- `sources.yml`


{% enddocs %}