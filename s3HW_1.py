from clickhouse_driver import Client

client = Client(host='localhost', # Use 'localhost' or '127.0.0.1' for a local server
user='default', # Default user, adjust if you've changed the user
password='', # Default installation has no password for 'default' user
port=9000) # Default TCP port for ClickHouse

# Attempt to execute a query
try:
    result = client.execute('SHOW TABLES')
    print(result)
except Exception as e:
    print(f"Encountered an error: {e}")


client.execute('SHOW TABLES')

client.execute('DROP DATABASE IF EXISTS my_books')
client.execute('CREATE DATABASE IF NOT EXISTS my_books')


client.execute('CREATE TABLE IF NOT EXISTS my_books.list (id Int64, Title String, Price Float64, Quantity Int64, Description String) ENGINE = MergeTree() ORDER BY id ')


import json

with open("books_from_books.toscrape.com.json", "r", encoding='utf-8') as file:
    data = json.load(file)


for item in data:
    client.execute("""
INSERT INTO my_books.list (
id, Title, Price,
Quantity, Description) VALUES""",
[(item['ID'],
item['Название'] or "",
item['Цена в фунтах стерлингов'],
item['Количество в наличии'],
item['Описание'] or "")])


result = client.execute("SELECT * FROM my_books.list")
print("Вставленная запись:", result[0])