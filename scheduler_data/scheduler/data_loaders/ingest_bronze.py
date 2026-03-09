import pandas as pd
from datetime import datetime, timezone

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    year = int(kwargs.get('year', 2021))
    month = str(kwargs.get('month', '01')).zfill(2)
    service_type = str(kwargs.get('service_type', 'yellow')).strip().lower()

    if service_type not in ['yellow', 'green']:
        raise ValueError("service_type debe ser 'yellow' o 'green'")

    url_parquet = (
        f'https://d37ci6vzurychx.cloudfront.net/trip-data/'
        f'{service_type}_tripdata_{year}-{month}.parquet'
    )

    print(f'Descargando: {url_parquet}')
    df = pd.read_parquet(url_parquet)
    print(f' Datos descargados con forma: {df.shape}')

    df['service_type'] = service_type
    df['source_month'] = f'{year}-{month}'
    df['ingest_ts'] = datetime.now(timezone.utc)

    rename_map = {
        'vendorid': 'VendorID',
        'vendorID': 'VendorID',
        'vendor_id': 'VendorID',
        'pulocationid': 'PULocationID',
        'dolocationid': 'DOLocationID',
        'ratecodeid': 'RatecodeID',
        'airport_fee ': 'airport_fee',
    }
    df = df.rename(columns={c: rename_map.get(c, c) for c in df.columns})

    expected_cols = [
        'service_type', 'source_month', 'ingest_ts',
        'VendorID',
        'tpep_pickup_datetime', 'tpep_dropoff_datetime',
        'lpep_pickup_datetime', 'lpep_dropoff_datetime',
        'pickup_datetime', 'dropoff_datetime',
        'passenger_count', 'trip_distance', 'RatecodeID',
        'store_and_fwd_flag', 'PULocationID', 'DOLocationID',
        'payment_type', 'fare_amount', 'extra', 'mta_tax',
        'tip_amount', 'tolls_amount', 'improvement_surcharge',
        'total_amount', 'congestion_surcharge', 'airport_fee',
        'ehail_fee', 'trip_type'
    ]

    for col in expected_cols:
        if col not in df.columns:
            df[col] = None

    df = df[expected_cols]

    print(f' DataFrame listo para bronze: {df.shape}')
    print(df.head(3))

    return df


@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'
    assert len(output) > 0, 'El DataFrame está vacío'
    required = ['service_type', 'source_month', 'ingest_ts']
    for c in required:
        assert c in output.columns, f'Falta columna requerida: {c}'
