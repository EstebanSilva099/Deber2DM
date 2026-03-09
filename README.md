# PSet 2 - Gold Mart con dbt + Análisis de negocio
# Esteban Silva 00329204
# https://github.com/EstebanSilva099/Deber2DM
## Descripción general del proyecto
Este proyecto construye un pequeño data mart de viajes de taxi usando una arquitectura por capas:

- Bronze: ingesta cruda
- Silver: datos limpios y estandarizados
- Gold: modelo dimensional para análisis de negocio

El objetivo final fue crear un star schema en Gold y responder 20 preguntas de negocio usando únicamente tablas gold.*.

---

## Tecnologías usadas

- Docker
- PostgreSQL
- pgAdmin
- Mage
- dbt
- SQL
- Jupyter Notebook (data_analysis.ipynb)

---

## Estructura general del repositorio

Deber2/
├── docker-compose.yaml
├── Dockerfile
├── requirements.txt
├── README.md
├── data_analysis.ipynb
├── scheduler_data/
├── warehouse_data/
├── warehouse_ui_data/
└── dbt/
    └── scheduler_transform/
        ├── dbt_project.yml
        ├── profiles.yml
        └── models/
            ├── bronze/
            ├── silver/
            └── gold/

---

## Arquitectura por capas

### Bronze
Bronze almacena los datos crudos ingeridos con la mínima transformación posible.

Objetivo principal:
- preservar los registros originales
- mantener timestamps de ingesta
- permitir trazabilidad

### Silver
Silver estandariza y prepara los datos para análisis.

Objetivo principal:
- limpiar nombres de columnas
- alinear esquemas entre tipos de servicio
- enriquecer datos de viajes
- preparar tablas auxiliares como las zonas de taxi

### Gold
Gold contiene el modelo dimensional final usado para análisis de negocio.

Objetivo principal:
- exponer tablas amigables para negocio
- soportar agregaciones e indicadores
- responder las 20 preguntas de negocio

---

## Star schema en Gold

### Tabla de hechos
- analitics_gold.fact_trips

### Tablas de dimensiones
- analitics_gold.dim_date
- analitics_gold.dim_service_type
- analitics_gold.dim_payment_type
- analitics_gold.dim_vendor
- analitics_gold.dim_zone

### Granularidad
1 fila en fact_trips = 1 viaje de taxi

---

## Estructura del star schema

                    dim_date
                       -
                       -
dim_service_type --- fact_trips --- dim_payment_type
                       -
                       -
                   dim_vendor
                       -
                       -
                    dim_zone
            -                   -
        pickup zone         dropoff zone

### Notas
- dim_zone se usa dos veces en la tabla de hechos:
  - pu_location_key para pickup
  - do_location_key para dropoff
- trip_fact_key fue creada como una llave técnica única para cada viaje

---

## Descripción de tablas Gold

### dim_date
Dimensión calendario usada para analizar viajes por año y mes.

Columnas principales:
- date_key
- date_day
- year
- month
- day
- day_of_week

### dim_service_type
Dimensión del tipo de servicio.

Columnas principales:
- service_type_key
- service_type

Valores posibles:
- yellow
- green

### dim_payment_type
Dimensión del tipo de pago.

Columnas principales:
- payment_type_key
- payment_type

Ejemplos:
- cash
- card
- other

### dim_vendor
Dimensión de proveedor.

Columnas principales:
- vendor_key
- vendor_id

### dim_zone
Dimensión de zonas de taxi.

Columnas principales:
- zone_key
- location_id
- borough
- zone
- service_zone

### fact_trips
Tabla de hechos para todo el análisis de negocio a nivel de viaje.

Columnas principales:
- trip_fact_key
- service_type_key
- payment_type_key
- vendor_key
- pickup_date_key
- pu_location_key
- do_location_key
- source_month
- pickup_at
- dropoff_at
- passenger_count
- trip_distance
- trip_duration_min
- fare_amount
- tip_amount
- total_amount

---

## Alcance de datos usado en el análisis

Aunque algunas preguntas de negocio estaban redactadas como ejemplo para 2024, el dataset cargado en la capa Gold contiene principalmente:

- enero 2021
- febrero 2021
- una cantidad muy pequeña de registros en marzo 2021
- tanto registros green como yellow

Por eso, todas las interpretaciones se hicieron sobre el periodo realmente disponible en analitics_gold.

---

## Preguntas de negocio respondidas

Las 20 preguntas fueron respondidas en:
- data_analysis.ipynb

Cada respuesta incluye:
- query SQL
- tablas usadas
- interpretación corta

### Secciones cubiertas

#### 1. Demanda / estacionalidad
1. Viajes por mes
2. Viajes por service_type y mes
3. Top 10 zonas de pickup
4. Top 10 zonas de dropoff
5. Top 5 boroughs por mes (pickup)
6. Horas pico por día de semana
7. Distribución de viajes por día de semana

#### 2. Ingresos / tarifas / propinas
8. Ingreso total por mes
9. Ingreso total por service_type y mes
10. tip % promedio por mes
11. tip % por borough y mes
12. Top 10 zonas de pickup por ingreso total
13. Top 10 zonas de pickup por tip % con mínimo N viajes
14. Comparación cash vs card

#### 3. Duración / distancia / eficiencia
15. Duración promedio por mes
16. Distancia promedio por mes
17. Velocidad promedio por borough y franja horaria
18. p50 y p90 de duración por borough
19. Top 10 zonas de pickup por p90 de duración

#### 4. Rutas / patrones
20. Top 10 rutas borough a borough por número de viajes

---

## Hallazgos principales

Algunos de los resultados más importantes del análisis fueron:

- La demanda se concentra principalmente en enero y febrero de 2021
- Yellow domina a Green tanto en volumen de viajes como en ingresos
- Manhattan concentra la mayor parte de los pickups y dropoffs
- JFK Airport es la zona de pickup con mayor ingreso total
- Card domina a cash en viajes, revenue y propinas registradas
- La demanda pico tiende a concentrarse en la tarde
- Friday es el día de mayor demanda
- La ruta más frecuente es Manhattan -> Manhattan
- Staten Island presenta las mayores duraciones de viaje en el análisis por percentiles

---

## Cómo ejecutar el proyecto

### 1. Levantar servicios
docker compose up -d

### 2. Abrir herramientas
- Mage
- pgAdmin
- PostgreSQL warehouse

### 3. Ejecutar transformaciones
Ejecutar los modelos o pipelines de Bronze, Silver y Gold.

### 4. Validar tablas Gold
Tablas principales a validar:
- analitics_gold.dim_date
- analitics_gold.dim_service_type
- analitics_gold.dim_payment_type
- analitics_gold.dim_vendor
- analitics_gold.dim_zone
- analitics_gold.fact_trips

### 5. Ejecutar análisis
Abrir:
- data_analysis.ipynb

y ejecutar las queries SQL usadas para responder las 20 preguntas de negocio.

---

## Queries de validación de ejemplo

### Ver tablas Gold
select table_name
from information_schema.tables
where table_schema = 'analitics_gold'
order by table_name;

### Vista previa de la fact table
select *
from analitics_gold.fact_trips
limit 10;

### Viajes por mes
select
    d.year,
    d.month,
    count(*) as total_trips
from analitics_gold.fact_trips f
join analitics_gold.dim_date d
    on f.pickup_date_key = d.date_key
group by d.year, d.month
order by d.year, d.month;

---

## Entregables incluidos

- modelos dbt para Bronze, Silver y Gold
- star schema en Gold
- data_analysis.ipynb
- queries SQL para 20 preguntas de negocio
- este archivo de documentación

---
## Cpaturas de pantalla
<img width="1917" height="967" alt="image" src="https://github.com/user-attachments/assets/4453cb82-d900-4bf0-95a1-17d1c6f9b358" />
<img width="1917" height="643" alt="image" src="https://github.com/user-attachments/assets/cbb00492-8963-4e64-acd7-cce15c02643c" />
<img width="1919" height="1032" alt="image" src="https://github.com/user-attachments/assets/00be02bb-74e1-4d2d-99b6-352402863b01" />
<img width="1019" height="544" alt="image" src="https://github.com/user-attachments/assets/2752824f-bfe5-4b51-8a1d-f92c39865402" />
<img width="1919" height="916" alt="image" src="https://github.com/user-attachments/assets/752769c2-e79e-47eb-986e-39a86fef9d24" />
<img width="1918" height="1013" alt="image" src="https://github.com/user-attachments/assets/b6664507-8735-48c0-aa24-d9204c92b5e3" />
<img width="1919" height="877" alt="image" src="https://github.com/user-attachments/assets/4291e234-0cff-412b-8e9f-d8c486cb0ce0" />
<img width="1919" height="873" alt="image" src="https://github.com/user-attachments/assets/76b08bb5-f833-4591-8756-6bd3cb48e134" />

## Conclusión final

Este proyecto logró construir correctamente un mart dimensional en Gold a partir de datos de viajes de taxi usando PostgreSQL, Mage y dbt.
El modelo final soporta análisis de negocio mediante un star schema simple centrado en fact_trips, con dimensiones reutilizables para fecha, zona, tipo de pago, vendor y tipo de servicio.
Usando únicamente tablas gold.*, el proyecto respondió 20 preguntas de negocio e identificó patrones claros en demanda, ingresos, propinas, eficiencia de viajes y rutas.

## Cheklist
 *   Docker Compose levanta Postgres + Mage
 *   Credenciales en Mage Secrets y .env (solo .env.example en repo)
 *   Pipeline ingest_bronze mensual e idempotente + tabla de cobertura
 *   dbt corre dentro de Mage: dbt_build_silver, dbt_build_gold, quality_checks
 *   Silver materialized = views; Gold materialized = tables
 *   Gold tiene esquema estrella completo
 *   Particionamiento: RANGE en fct_trips, HASH en dim_zone, LIST en dim_service_type y dim_payment_type
 *   README incluye \d+ y EXPLAIN (ANALYZE, BUFFERS) con pruning
 *   dbt test pasa desde Mage
 *   Notebook responde 20 preguntas usando solo gold.*
 *   Triggers configurados y evidenciados
