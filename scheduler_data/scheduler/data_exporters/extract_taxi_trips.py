import psycopg2
from io import StringIO
from pandas import DataFrame
from mage_ai.data_preparation.shared.secrets import get_secret_value

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs) -> None:
    if df is None or df.empty:
        raise ValueError('El DataFrame recibido está vacío')

    schema_name = 'bronze'
    table_name = 'taxi_trips'

    source_month = str(df['source_month'].iloc[0])
    service_type = str(df['service_type'].iloc[0])

    host = get_secret_value("POSTGRES_HOST")
    port = int(get_secret_value("POSTGRES_PORT"))
    dbname = get_secret_value("POSTGRES_DB")
    user = get_secret_value("POSTGRES_USER")
    password = get_secret_value("POSTGRES_PASSWORD")

    print(f"   Conectando a Postgres...")
    print(f"   host={host} port={port} db={dbname} user={user}")
    print(f"   exportando {len(df)} filas | month={source_month} | service={service_type}")

    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
    )
    conn.autocommit = True

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database(), current_user;")
            db_info = cur.fetchone()
            print(f" Conectado a DB real: {db_info}")

            cur.execute("""
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema IN ('bronze', 'raw', 'silver', 'gold')
                ORDER BY 1,2;
            """)
            tablas = cur.fetchall()
            print(f" Tablas visibles: {tablas}")

            cur.execute(f"""
                CREATE SCHEMA IF NOT EXISTS {schema_name};

                CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                    service_type TEXT NOT NULL,
                    source_month TEXT NOT NULL,
                    ingest_ts TIMESTAMPTZ NOT NULL,

                    "VendorID" BIGINT NULL,
                    tpep_pickup_datetime TIMESTAMP NULL,
                    tpep_dropoff_datetime TIMESTAMP NULL,
                    lpep_pickup_datetime TIMESTAMP NULL,
                    lpep_dropoff_datetime TIMESTAMP NULL,
                    pickup_datetime TIMESTAMP NULL,
                    dropoff_datetime TIMESTAMP NULL,
                    passenger_count NUMERIC NULL,
                    trip_distance NUMERIC NULL,
                    "RatecodeID" NUMERIC NULL,
                    store_and_fwd_flag TEXT NULL,
                    "PULocationID" BIGINT NULL,
                    "DOLocationID" BIGINT NULL,
                    payment_type NUMERIC NULL,
                    fare_amount NUMERIC NULL,
                    extra NUMERIC NULL,
                    mta_tax NUMERIC NULL,
                    tip_amount NUMERIC NULL,
                    tolls_amount NUMERIC NULL,
                    improvement_surcharge NUMERIC NULL,
                    total_amount NUMERIC NULL,
                    congestion_surcharge NUMERIC NULL,
                    airport_fee NUMERIC NULL,
                    ehail_fee NUMERIC NULL,
                    trip_type NUMERIC NULL
                );
            """)
            print(" Schema/tabla asegurados")

            cur.execute(f"""
                DELETE FROM {schema_name}.{table_name}
                WHERE source_month = %s
                  AND service_type = %s;
            """, (source_month, service_type))
            print(" Delete previo ejecutado (idempotencia)")

        from sqlalchemy import create_engine

        engine = create_engine(
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
        )
        
        df = df.copy()
        df.columns = [str(c).strip().lower() for c in df.columns]

        print(" Columnas normalizadas para insertar:")
        print(df.columns.tolist())

        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema_name,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=10000,
        )
        print(" Exportación completada")

    finally:
        conn.close()

