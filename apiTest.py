from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import APIKeyHeader
import pandas as pd
import pyodbc
from azure.storage.blob import BlobServiceClient
from io import StringIO, BytesIO
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, select, DateTime, text
from sqlalchemy.orm import sessionmaker
import urllib
import fastavro

# Inicializa la app FastAPI
app = FastAPI()

# Seguridad: Definir una clave API
API_KEY = "M423KL4z"
api_key_header = APIKeyHeader(name="X-API-KEY")

# Función para verificar la clave API
def verify_api_key(api_key: str = Depends(api_key_header)):
    if api_key != API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Clave API no válida",
        )

# Configura la conexión a Azure Blob Storage
AZURE_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=cs210032000920bcfba;AccountKey=ZXutJVGegmMjPPq7ToEr0pZyTGalWka/JUTv8n4m5Sz5v8blHFnhzbqeGoLUgd4FkxTM7+QPLaDQsfTILCDysQ==;EndpointSuffix=core.windows.net"
blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
container_name = "filetest"

# Conexión a SQL Server
def get_db_connection():
    server = 'servertestgb.database.windows.net'
    database = 'DBTest'
    username = 'admin_sa'
    password = '156837IqBa'
    driver = '{ODBC Driver 17 for SQL Server}'
    connection_string = f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}'
    return connection_string

# Función para leer archivo CSV desde Azure Blob Storage
def read_csv_from_blob(blob_name: str):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_data = blob_client.download_blob().readall()
    csv_str = blob_data.decode('utf-8')
    return pd.read_csv(StringIO(csv_str))

#Función para guardar en un blob storage datos convertidos en avro
def backup_table(query,avro_schema, avro_file):
    # Conectar a SQL Server
    conn_str = get_db_connection()
    conn = pyodbc.connect(conn_str)
    df = pd.read_sql(query, conn)
    conn.close()

    # Convertir DataFrame a lista de diccionarios
    records = df.to_dict(orient='records')

    # Crear un stream en memoria para almacenar el archivo AVRO
    avro_buffer = BytesIO()

    # Escribir los datos en formato AVRO usando fastavro
    fastavro.writer(avro_buffer, avro_schema, records)
    avro_buffer.seek(0)  # Volver al inicio del stream para lectura

    # Conectar con Azure Blob Storage
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=avro_file)

    # Subir el archivo AVRO al Blob Storage
    blob_client.upload_blob(avro_buffer, overwrite=True)

    return(f'Archivo {avro_file} subido exitosamente al contenedor {container_name}')

# Leer el archivo AVRO y convertirlo a un DataFrame
def get_avro(avro_file):
    # Crear cliente de blob
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=avro_file)

    # Descargar el archivo AVRO a un stream en memoria
    avro_buffer = BytesIO()
    avro_buffer.write(blob_client.download_blob().readall())
    avro_buffer.seek(0)  # Volver al inicio del buffer

    # Leer los datos del stream y convertirlos en un DataFrame
    with avro_buffer as f:
        reader = fastavro.reader(f)
        records = [record for record in reader]

    return pd.DataFrame(records)

#Restaurar tabla en SQL Server
def restore_table(df,table_name,new):
    # Conexión a la base de datos SQL Server
    connection_string = get_db_connection()
    params_write = urllib.parse.quote_plus(connection_string)
    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params_write)
    
    if new:
        df.to_sql(table_name, engine, if_exists='replace', index=False)
    else: 
        df.to_sql(table_name, engine, if_exists='append', index=False)
        
    return {"message": f"Restauración exitosamente de la tabla {table_name}"}

#Crear tabla Employees
def create_table_employees():
    # Crear metadata y definir la tabla manualmente
    metadata = MetaData()
    table_name = "Employees"

    employees = Table(
        table_name, metadata,
        Column('Id', Integer, nullable=False),
        Column('Name', String(250), nullable=False),
        Column('DateHired', DateTime, nullable=True),
        Column('Department_Id', Integer, nullable=False),
        Column('Job_Id', Integer, nullable=False),
        Column('DateCreate', DateTime, nullable=False, server_default=text('GETDATE()')),
        Column('DateUpdate', DateTime, nullable=True)
    )

    # Conexión a la base de datos
    connection_string = get_db_connection()
    params_write = urllib.parse.quote_plus(connection_string)
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params_write}")

    # Crear o reemplazar la tabla
    metadata.drop_all(engine, [employees])  # Elimina la tabla si ya existe
    metadata.create_all(engine)  # Crea la tabla

#Crear tabla Departments
def create_table_departments():
    # Crear metadata y definir la tabla manualmente
    metadata = MetaData()
    table_name = "Departments"

    departments = Table(
        table_name, metadata,
        Column('Id', Integer, nullable=False),
        Column('Department', String(100), nullable=False)
    )

    # Conexión a la base de datos
    connection_string = get_db_connection()
    params_write = urllib.parse.quote_plus(connection_string)
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params_write}")

    # Crear o reemplazar la tabla
    metadata.drop_all(engine, [departments])  # Elimina la tabla si ya existe
    metadata.create_all(engine)  # Crea la tabla

#Crear tabla Jobs
def create_table_jobs():
    # Crear metadata y definir la tabla manualmente
    metadata = MetaData()
    table_name = "Jobs"

    jobs = Table(
        table_name, metadata,
        Column('Id', Integer, nullable=False),
        Column('Job', String(100), nullable=False)
    )

    # Conexión a la base de datos
    connection_string = get_db_connection()
    params_write = urllib.parse.quote_plus(connection_string)
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params_write}")

    # Crear o reemplazar la tabla
    metadata.drop_all(engine, [jobs])  # Elimina la tabla si ya existe
    metadata.create_all(engine)  # Crea la tabla

def insert_data_master(df,masterTable):
    # Conexión a la base de datos SQL Server
    connection_string = get_db_connection()
    params_write = urllib.parse.quote_plus(connection_string)
    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params_write)

    # Crear una sesión para interactuar con la base de datos
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # Cargar la tabla existente desde la base de datos
    metadata = MetaData()
    tabla = Table(masterTable, metadata, autoload_with=engine)

    # Validar y agregar solo los datos nuevos
    for _, row in df.iterrows():
        # Consulta para verificar si el registro ya existe
        query = select(tabla).where(tabla.c.Id == row['Id'])
        result = session.execute(query).fetchone()

        # Si el registro no existe, lo insertamos
        if not result:
            session.execute(tabla.insert().values(**row.to_dict()))
            #print("Datos insertados con éxito.")
        #else:
            #print(f"Registro con id={row['Id']} ya existe. No se inserta.")
 
    # Cerrar la sesión
    session.commit()
    session.close() 

# Insertar datos de empleados
@app.post("/insert_hired_employees", dependencies=[Depends(verify_api_key)])
def insert_hired_employees():
    #Crear tabla en la base de datos
    create_table_employees()
    # Conexión a la base de datos SQL Server
    connection_string = get_db_connection()
    params_write = urllib.parse.quote_plus(connection_string)
    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params_write)
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor() 

    # Lee el archivo CSV desde Azure Blob Storage
    file = "hired_employees.csv"
    df = read_csv_from_blob(file)
    # Asignar encabezados
    df.columns = ['id', 'name', 'datetime','department_id','job_id']
  
    # Insertar en lotes de 1000 registros
    batch_size = 1000

    try:
        for i in range(0, len(df), batch_size):
            # Tomar un lote de 1000 filas
            batch = df.iloc[i:i + batch_size]

            # Ejecutar la inserción del lote
            batch.to_sql('Hired', engine, if_exists='replace', index=False)

            # Ejecutar un procedimiento almacenado
            cursor.execute("exec [dbo].[MergeEmployees]")

            # Confirmar los cambios en cada lote
            conn.commit()
            #print(f'Lote {i // batch_size + 1} insertado con éxito.')

        return {"message": f"Datos del archivo hired_employees.csv procesados exitosamente"}
    except pyodbc.Error as e:
        print(f'Error al insertar registros: {e}')
        conn.rollback()

    finally:
        # Cerrar la conexión y el cursor
        cursor.close()
        conn.close()
        engine.dispose()

#Insertar el maestro de departamentos
@app.post("/insert_departments", dependencies=[Depends(verify_api_key)])
def insert_departments():
    #Crear tabla en la base de datos
    create_table_departments()
    # Lee el archivo CSV desde Azure Blob Storage
    file = "departments.csv"
    df = read_csv_from_blob(file)

    # Asignar encabezados
    df.columns = ['Id','Department']
    masterTable = 'Departments'
    insert_data_master(df,masterTable)

    return {"message": f"Datos del archivo departments.csv procesados exitosamente"}

#Insertar el maestro de trabajos
@app.post("/insert_jobs", dependencies=[Depends(verify_api_key)])
def insert_jobs():
    #Crear tabla en la base de datos
    create_table_jobs()
    # Lee el archivo CSV desde Azure Blob Storage
    file = "jobs.csv"
    df = read_csv_from_blob(file)

    # Asignar encabezados
    df.columns = ['Id','Job']
    masterTable = 'Jobs'
    insert_data_master(df,masterTable)

    return {"message": f"Datos del archivo jobs.csv procesados exitosamente"}

#Crear backup de la tabla Employees
@app.post("/create_backup_employees", dependencies=[Depends(verify_api_key)])
def create_backup_employees():
    avro_schema = {
    "type": "record",
    "name": "hired_employees",
    "fields": [
        {"name": "Id", "type": "int"},
        {"name": "Name", "type": "string"},
        {"name": "DateHired", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "Department_Id", "type": "int"},
        {"name": "Job_Id", "type": "int"}
        ]
    }
    avro_file = "employees.avro"
    query = "select Id, Name, isnull(DateHired,'1900/01/01') as DateHired, Department_Id, Job_Id from Employees"
    response = backup_table(query,avro_schema, avro_file)
    return response
    
#Crear backup de la tabla Departments
@app.post("/create_backup_departments", dependencies=[Depends(verify_api_key)])
def create_backup_departments():
    avro_schema = {
    "type": "record",
    "name": "Departments",
    "fields": [
        {"name": "Id", "type": "int"},
        {"name": "Department", "type": "string"}
        ]
    }
    avro_file = "departments.avro"
    query = "select Id, Department from Departments"
    response = backup_table(query,avro_schema, avro_file)
    return response

#Crear backup de la tabla Jobs
@app.post("/create_backup_jobs", dependencies=[Depends(verify_api_key)])
def create_backup_jobs():
    avro_schema = {
    "type": "record",
    "name": "Jobs",
    "fields": [
        {"name": "Id", "type": "int"},
        {"name": "Job", "type": "string"}
        ]
    }
    avro_file = "jobs.avro"
    query = "select Id, Job from Jobs"
    response = backup_table(query,avro_schema, avro_file)
    return response

#Restaurar el backup de la tabla Employees
@app.post("/restore_employees", dependencies=[Depends(verify_api_key)])
def restore_employees():
    avro_file = "employees.avro"
    table_name = "Employees"
    create_table_employees()
    df = get_avro(avro_file)

    # Eliminar la información de zona horaria (tz-aware) en DateHired
    df['DateHired'] = pd.to_datetime(df['DateHired'], errors='coerce').dt.tz_localize(None)

    response = restore_table(df,table_name,False)
    return response

#Restaurar el backup de la tabla Departments
@app.post("/restore_departments", dependencies=[Depends(verify_api_key)])
def restore_departments():
    avro_file = "departments.avro"
    table_name = "Departments"
    df = get_avro(avro_file)

    response = restore_table(df,table_name,True)
    return response

#Restaurar el backup de la tabla Departments
@app.post("/restore_jobs", dependencies=[Depends(verify_api_key)])
def restore_jobs():
    avro_file = "jobs.avro"
    table_name = "Jobs"
    df = get_avro(avro_file)

    response = restore_table(df,table_name,True)
    return response