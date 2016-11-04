from sqlalchemy import engine_from_config
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

config = {'sqlalchemy.url': 'postgresql:///encoded'}
engine = engine_from_config(config)
session = sessionmaker(bind=engine)()
connection = session.connection()

tuples = []
with open('uuids_list.txt', 'r') as ids:
    for entry in ids:
        arr = entry.strip().split('\t')
        tuples.append((arr[0], arr[1]))


for (ty, uuid) in tuples:
    stmt = text("SELECT propsheets.properties FROM propsheets, resources, current_propsheets WHERE propsheets.rid = '" + uuid + "' AND propsheets.rid = resources.rid AND current_propsheets.sid = propsheets.sid;")
    properties = connection.execute(stmt)
    print (ty)
    for x in properties:
        print (x)
        print ('******************')
    print ('--------------------')

'''(example files are app.py in /develop/snovault/src/snovault
    and base.ini - from /encode/)
>> the basic thing: 

engine = engine_from_config(settings, 'sqlalchemy.', **engine_opts)
    if engine.url.drivername == 'postgresql':
        timeout = settings.get('postgresql.statement_timeout')
        if timeout:
            timeout = int(timeout) * 1000
            set_postgresql_statement_timeout(engine, timeout)
    return engine


then use the engine


sqlalchemy.url = postgresql:///encoded

sudo pip3 install psycopg2
sudo pip3 install sqlalchemy




>>> from sqlalchemy.orm import sessionmaker
>>> Session = sessionmaker(bind=engine)
>>> session = Session()

>>> config = {'sqlalchemy.url': 'postgresql:///encoded'}
>>> engine = engine_from_config(config)
>>> session = sessionmaker(bind=engine)()
>>> connection = session.connection()
>>> result = connection.execute("delete from current_propsheets where current_propsheets.rid = '46133e3d-5130-4f6b-9424-521b05449146'; ")
>>> result = connection.execute("delete from keys where keys.rid = '46133e3d-5130-4f6b-9424-521b05449146'; ")
>>> result = connection.execute("delete from propsheets where propsheets.rid = '46133e3d-5130-4f6b-9424-521b05449146'; ")
>>> result = connection.execute("delete from links where links.source = '46133e3d-5130-4f6b-9424-521b05449146'; ")
>>> result = connection.execute("delete from links where links.target = '46133e3d-5130-4f6b-9424-521b05449146'; ")
>>> result = connection.execute("delete from resources where resources.rid = '46133e3d-5130-4f6b-9424-521b05449146'; ")
>>> result = connection.execute("select rid from resources where resources.rid = '46133e3d-5130-4f6b-9424-521b05449146'; ")
>>> for x in result:
...     print (x)
... 
>>> session.commit()

delete_table = Table('ids', meta, Column('id', String(60), nullable=False, key= 'name'))
>>> i = delete_table.insert()
>>> i.execute(name= 'ZOPA')
<sqlalchemy.engine.result.ResultProxy object at 0x7f619b51b7b8>
>>> s = delete_table.select()
>>> rs = s.execute()
>>> row = rs.fetchone()
>>> print ('Name:', row[0])
Name: ZOPA
>>> 

for table in reversed(meta.sorted_tables):
    engine.execute(table.delete())


from sqlalchemy.engine import reflection
from sqlalchemy import select, func, MetaData, create_engine

dburl = 'postgresql:///encoded'
engine = create_engine(dburl, client_encoding='utf8')
meta = MetaData()
meta.reflect(bind=engine)
for table in meta.sorted_tables:
    print (table)

for table in reversed(meta.sorted_tables):
    engine.execute(table.delete())
    
data = {}
for table in meta.sorted_tables:
    r = engine.execute(table.select())
    rows = [ dict(row.items()) for row in r ]
    data[table.name] = rows

'''
result = connection.execute("delete from current_propsheets where current_propsheets.rid = '46133e3d-5130-4f6b-9424-521b05449146'; ")

result = connection.execute("select rid from resources where resources.rid = '46133e3d-5130-4f6b-9424-521b05449146'; ")
from sqlalchemy import inspect
inspector = inspect(engine)
for table_name in inspector.get_table_names():
    print ('TABLE NAME : ' + table_name)
    for column in inspector.get_columns(table_name):
        print("Column: %s" % column['name'])
