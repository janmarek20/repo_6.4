from sqlalchemy import Table, Column, Integer, String, MetaData, Numeric, create_engine, ForeignKey, select, update, delete
from sqlalchemy import inspect
import pandas as pd

def create_tables(engine, meta):
    inspector = inspect(engine)
    table_names = inspector.get_table_names()

    if 'clean_stations' not in table_names:
        clean_stations = Table(
            'clean_stations', meta,
            Column('id', Integer, primary_key=True),
            Column('station', String),
            Column('latitude', Numeric),
            Column('longitude', Numeric),
            Column('elevation', Numeric),
            Column('name', String),
            Column('country', String),
            Column('state', String),
        )
        clean_stations.create(engine)

    if 'clean_measure' not in table_names:
        clean_measure = Table(
            'clean_measure', meta,
            Column('id', Integer, primary_key=True),
            Column('station', String, ForeignKey('clean_stations.station')),
            Column('date', String),
            Column('precip', Numeric),
            Column('tobs', Numeric)
        )
        clean_measure.create(engine)

def insert_data_stations(engine, df):
    conn = engine.connect()

    try:
        table = Table('clean_stations', MetaData(), autoload_with=engine)
        ins = table.insert()

        for row in df.itertuples(index=False):
            record_to_insert = {
                'station': row.station,
                'latitude': row.latitude,
                'longitude': row.longitude,
                'elevation': row.elevation,
                'name': row.name,
                'country': row.country,
                'state': row.state
            }
            conn.execute(ins, record_to_insert)

    finally:
        conn.close()

def insert_data_measure(engine, df):
    conn = engine.connect()

    try:
        table = Table('clean_measure', MetaData(), autoload_with=engine)
        ins = table.insert()

        for row in df.itertuples(index=False):
            record_to_insert = {
                'station': row.station,
                'date': row.date,
                'precip': row.precip,
                'tobs': row.tobs
            }
            conn.execute(ins, record_to_insert)

    finally:
        conn.close()

def read_data(engine, table, has_elevation=True):
    conn = engine.connect()

    try:
        stmt = select('*').select_from(table)
        results = conn.execute(stmt).fetchall()
        print(f"Reading all records from {table.name}:")
        for row in results:
            print(row)

        if has_elevation:
            stmt = select('*').where(
                table.c.elevation > 10
            )
            results = conn.execute(stmt).fetchall()
            print(f"Reading records with elevation > 10 from {table.name}:")
            for row in results:
                print(row)

    finally:
        conn.close()

def update_data(engine, table, update_column, new_value):
    conn = engine.connect()

    try:
        stmt = (
            update(table)
            .where(table.c.id == 1)
            .values({update_column: new_value})
        )
        conn.execute(stmt)
        print(f"Updated record in {table.name}")

    finally:
        conn.close()

def delete_data(engine, table):
    conn = engine.connect()

    try:
        stmt = delete(table).where(table.c.id == 2)
        conn.execute(stmt)
        print(f"Deleted record from {table.name}")

    finally:
        conn.close()

if __name__ == "__main__":
    df_stations = pd.read_csv('clean_stations.csv')
    df_measure = pd.read_csv('clean_measure.csv')

    engine = create_engine('sqlite:///database_6.4.db')
    meta = MetaData(bind=engine)

    create_tables(engine, meta)

    insert_data_stations(engine, df_stations)
    insert_data_measure(engine, df_measure.head(100))

    read_data(engine, Table('clean_stations', MetaData(), autoload_with=engine), has_elevation=True)
    update_data(engine, Table('clean_stations', MetaData(), autoload_with=engine), 'name', 'Nowa nazwa')
    delete_data(engine, Table('clean_stations', MetaData(), autoload_with=engine))

    read_data(engine, Table('clean_measure', MetaData(), autoload_with=engine), has_elevation=False)
    update_data(engine, Table('clean_measure', MetaData(), autoload_with=engine), 'tobs', 999)
    delete_data(engine, Table('clean_measure', MetaData(), autoload_with=engine))
