# Библиотеки
import requests
import psycopg2

conn = psycopg2.connect(
    host     = "localhost",
    database = "db",
    user     = "username",
    password = "password"
)

cur = conn.cursor()

# Создание сервера
def handle_request(request):
    number = int(request.data)

    cur.execute("SELECT * FROM data WHERE number = %s", (number,))

    if cur.fetchone() is not None:
        return "Ошибка: Число уже поступало"

    cur.execute("SELECT * FROM data ORDER BY id DESC LIMIT 1")
    last_number = cur.fetchone()[1]

    if number != last_number + 1:
        return "Ошибка: Было число на единицу больше"

    new_number = number + 1
    cur.execute("INSERT INTO data (number) VALUES (%s)", (new_number,))
    conn.commit()
    return str(new_number)

# Запуск
while True:
    request  = requests.get("http://localhost:8000")
    response = handle_request(request)

    requests.post("http://localhost:8000", data=response)