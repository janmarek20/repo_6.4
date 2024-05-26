from sqlalchemy import Table, Column, Integer, String, MetaData, Numeric, Date, ForeignKey
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd

# Table clean_stations

from sqlalchemy import Table, Column, Integer, String, MetaData, Numeric, Date, ForeignKey
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd

# Table clean_stations

df = pd.read_csv('clean_stations.csv')

engine = create_engine('sqlite:///database_6.4.db')
conn = engine.connect()

meta = MetaData()

inspector = inspect(engine)

if 'clean_stations' in inspector.get_table_names():
    conn.execute("DROP TABLE clean_stations")

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

meta.create_all(engine)

print(inspector.get_table_names())

ins = clean_stations.insert()

try:
    for index, row in df.iterrows():
        record_to_insert = {
            'station': row['station'],
            'latitude': row['latitude'],
            'longitude': row['longitude'],
            'elevation': row['elevation'],
            'name': row['name'],
            'country': row['country'],
            'state': row['state']
        }
        conn.execute(ins, record_to_insert)

    result = conn.execute("SELECT * FROM clean_stations LIMIT 5").fetchall()
    print(result)
finally:
    conn.close()

# Table clean_measure

df_2 = pd.read_csv('clean_measure.csv')

conn = engine.connect()

if 'clean_measure' in inspector.get_table_names():
    conn.execute("DROP TABLE clean_measure")

clean_measure = Table(
    'clean_measure', meta,
    Column('id', Integer, primary_key=True),
    Column('station', String, ForeignKey('clean_stations.station')),
    Column('date', String),
    Column('precip', Numeric),
    Column('tobs', Numeric)
)

meta.create_all(engine)

print(inspector.get_table_names())

ins = clean_measure.insert()

try:
    for index, row in df_2.iterrows():
        if index < 100:
            record_to_insert = {
                'station': row['station'],
                'date': row['date'],
                'precip': row['precip'],
                'tobs': row['tobs'],
            }
            conn.execute(ins, record_to_insert)

    result = conn.execute("SELECT * FROM clean_measure LIMIT 5").fetchall()
    print(result)
finally:
    conn.close()


