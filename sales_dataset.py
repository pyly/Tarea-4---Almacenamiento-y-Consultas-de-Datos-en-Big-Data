import happybase
import pandas as pd
from datetime import datetime

# Configuración
table_name = 'data_science_salaries'
file_path = '/mnt/data/online_sales_dataset.csv'

# Función para limpiar el dataset
def limpiar_dataset(file_path):
    """
    Limpia el dataset eliminando o reemplazando valores nulos y garantizando que los datos sean consistentes.
    """
    data = pd.read_csv(file_path)
    # Reemplazar valores nulos con valores predeterminados
    data['WarehouseLocation'] = data['WarehouseLocation'].fillna("Unknown")
    data['ShipmentProvider'] = data['ShipmentProvider'].fillna("Unknown")
    data['CustomerID'] = data['CustomerID'].fillna(0).astype(int)  # Reemplazar NaN con 0
    data['InvoiceDate'] = data['InvoiceDate'].fillna("1970-01-01")  # Fecha predeterminada
    # Convertir columnas numéricas a tipo adecuado
    data['Quantity'] = data['Quantity'].fillna(0).astype(int)
    data['UnitPrice'] = data['UnitPrice'].fillna(0).astype(float)
    return data

# Función para gestionar la tabla en HBase
def gestionar_tabla(connection, table_name):
    """
    Verifica si la tabla existe. Si existe, la elimina y la recrea con las familias de columnas necesarias.
    """
    if table_name.encode() in connection.tables():
        connection.delete_table(table_name, disable=True)
        print(f"Tabla '{table_name}' eliminada.")
    families = {
        'sales_data': dict(),  # Información de ventas
        'product_data': dict(),  # Información del producto
        'metadata': dict()  # Información adicional
    }
    connection.create_table(table_name, families)
    print(f"Tabla '{table_name}' creada exitosamente.")

# Función para cargar los datos desde el dataset a HBase
def cargar_datos(connection, table_name, data):
    """
    Carga los datos del dataset limpio a la tabla en HBase.
    """
    table = connection.table(table_name)
    for index, row in data.iterrows():
        row_key = f"row_{index}".encode()
        table.put(row_key, {
            b'sales_data:InvoiceNo': str(row['InvoiceNo']).encode(),
            b'sales_data:InvoiceDate': row['InvoiceDate'].encode(),
            b'sales_data:Quantity': str(row['Quantity']).encode(),
            b'sales_data:CustomerID': str(row['CustomerID']).encode(),
            b'sales_data:Country': row['Country'].encode(),
            b'product_data:StockCode': row['StockCode'].encode(),
            b'product_data:Description': row['Description'].encode(),
            b'product_data:UnitPrice': str(row['UnitPrice']).encode(),
            b'product_data:Category': row['Category'].encode(),
            b'metadata:PaymentMethod': row['PaymentMethod'].encode(),
            b'metadata:ShippingCost': str(row['ShippingCost']).encode(),
            b'metadata:SalesChannel': row['SalesChannel'].encode(),
            b'metadata:ReturnStatus': row['ReturnStatus'].encode(),
            b'metadata:ShipmentProvider': row['ShipmentProvider'].encode(),
            b'metadata:WarehouseLocation': row['WarehouseLocation'].encode(),
            b'metadata:OrderPriority': row['OrderPriority'].encode(),
        })
    print("Datos cargados exitosamente en HBase.")

# Función para mostrar las primeras filas de la tabla
def recorrer_tabla(connection, table_name):
    """
    Escanea y muestra las primeras 5 filas de la tabla en HBase.
    """
    table = connection.table(table_name)
    print("\n=== Primeras 5 filas de la tabla ===")
    count = 0
    for key, data in table.scan():
        print(f"Row Key: {key.decode()}")
        for col, val in data.items():
            print(f"{col.decode()}: {val.decode()}")
        print("-" * 40)
        count += 1
        if count >= 5:
            break

# Función para calcular el total de ventas por región
def total_ventas_por_region(connection, table_name):
    """
    Calcula el total de ventas por región sumando el monto de ventas (cantidad * precio unitario).
    """
    table = connection.table(table_name)
    ventas_por_region = {}
    for key, data in table.scan():
        region = data[b'sales_data:Country'].decode()
        quantity = int(data[b'sales_data:Quantity'].decode())
        unit_price = float(data[b'product_data:UnitPrice'].decode())
        sales_amount = quantity * unit_price
        ventas_por_region[region] = ventas_por_region.get(region, 0) + sales_amount
    print("\n=== Total de ventas por región ===")
    for region, total in ventas_por_region.items():
        print(f"{region}: ${total:.2f}")

# Función para encontrar los productos más vendidos
def top_productos(connection, table_name):
    """
    Encuentra los productos más vendidos por cantidad.
    """
    table = connection.table(table_name)
    product_sales = {}
    for key, data in table.scan():
        product = data[b'product_data:Description'].decode()
        quantity = int(data[b'sales_data:Quantity'].decode())
        product_sales[product] = product_sales.get(product, 0) + quantity
    top_products = sorted(product_sales.items(), key=lambda x: x[1], reverse=True)[:5]
    print("\n=== Productos más vendidos ===")
    for product, total in top_products:
        print(f"{product}: {total} unidades")

# Función para calcular el promedio de ventas por categoría
def promedio_ventas_por_categoria(connection, table_name):
    """
    Calcula el promedio de ventas por categoría de producto.
    """
    table = connection.table(table_name)
    ventas_por_categoria = {}
    cantidades_por_categoria = {}
    for key, data in table.scan():
        category = data[b'product_data:Category'].decode()
        quantity = int(data[b'sales_data:Quantity'].decode())
        unit_price = float(data[b'product_data:UnitPrice'].decode())
        sales_amount = quantity * unit_price
        ventas_por_categoria[category] = ventas_por_categoria.get(category, 0) + sales_amount
        cantidades_por_categoria[category] = cantidades_por_categoria.get(category, 0) + 1
    print("\n=== Promedio de ventas por categoría de producto ===")
    for category in ventas_por_categoria:
        avg_sales = ventas_por_categoria[category] / cantidades_por_categoria[category]
        print(f"{category}: ${avg_sales:.2f}")



# Ejecución del script
if __name__ == "__main__":
    # Limpiar el dataset
    data = limpiar_dataset(file_path)

    # Conexión a HBase
    connection = happybase.Connection('localhost')

    # Gestionar la tabla (eliminar y recrear si existe)
    gestionar_tabla(connection, table_name)

    # Cargar los datos limpios en HBase
    cargar_datos(connection, table_name, data)

    # Mostrar las primeras filas de la tabla
    recorrer_tabla(connection, table_name)

    # Ejecutar análisis
    total_ventas_por_region(connection, table_name)
    top_productos(connection, table_name)
    promedio_ventas_por_categoria(connection, table_name)

    # Cerrar la conexión
    connection.close()
