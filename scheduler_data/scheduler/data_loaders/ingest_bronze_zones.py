import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load_taxi_zones(*args, **kwargs):
    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
    print(f"Descargando taxi zones desde: {url}")

    df = pd.read_csv(url)
    df.columns = [c.strip().lower() for c in df.columns] 

    print(f"Taxi zones descargado: {df.shape}")
    print(df.head(3))
    return df
