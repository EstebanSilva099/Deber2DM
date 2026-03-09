import psycopg2
from pandas import DataFrame
from mage_ai.data_preparation.shared.secrets import get_secret_value

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


def get_secret_fallback(*keys):
    for k in keys:
        v = get_secret_value(k)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return None


@data_exporter
def export_zones_to_postgres(df: DataFrame, **kwargs) -> None:
    if df is None or df.empty:
        raise ValueError("DataFrame vacío")

    host = get_secret_fallback("POSTGRES_HOST")
    port = int(get_secret_fallback("POSTGRES_PORT"))
    dbname = get_secret_fallback("POSTGRES_DB", "POSTGRES_DB")
    user = get_secret_fallback("POSTGRES_USER")
    password = get_secret_fallback("POSTGRES_PASSWORD")

    conn = psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user, password=password
    )
    conn.autocommit = True

    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE SCHEMA IF NOT EXISTS bronze;

                CREATE TABLE IF NOT EXISTS bronze.taxi_zones (
                    locationid    INT PRIMARY KEY,
                    borough       TEXT,
                    zone          TEXT,
                    service_zone  TEXT
                );

                TRUNCATE TABLE bronze.taxi_zones;
            """)
            print(" Tabla bronze.taxi_zones lista (truncate hecho)")

        from sqlalchemy import create_engine
        engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}")

        df = df.copy()
        df.columns = [str(c).strip().lower() for c in df.columns]

        df = df[['locationid', 'borough', 'zone', 'service_zone']]

        df.to_sql(
            name='taxi_zones',
            con=engine,
            schema='bronze',
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000,
        )

        print(" Exportación taxi_zones completada")
    finally:
        conn.close()
