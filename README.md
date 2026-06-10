# Лабораторная работа 3/1: Асинхронное взаимодействие микросервисов через RabbitMQ + gRPC (Google Remote Procedure Call)

## Постановка задачи

Необходимо разработать систему асинхронной обработки запросов на основе брокера сообщений RabbitMQ и gRPC.  
Система состоит из трёх независимых компонентов:

- **Producer** – отправляет задачи в очередь RabbitMQ.
- **Consumer** – забирает задачи из очереди и вызывает gRPC‑сервер.
- **gRPC Server** – выполняет бизнес‑логику и возвращает результат.

Такой подход позволяет развязать сервисы и повысить отказоустойчивость.

---

## Вариант 11

| № | Задание | Результат |
|---|---------|-----------|
| 1 | Регистрация пользователя. Producer отправляет email и пароль. | `Пользователь {email} зарегистрирован` |
| 2 | Определение MIME‑типа. Producer отправляет имя файла. | MIME‑тип (например, `image/jpeg`) |
| 3 | Разбиение текста на предложения. Producer отправляет абзац. | Список предложений |

---

## Архитектура системы

### Часть 1. Синхронное взаимодействие (gRPC)

Клиент и сервер общаются напрямую по протоколу gRPC в режиме запрос‑ответ.  
Этот этап является базовым для проверки логики обработки.

**Диаграмма:**
[gRPC Client] ---> [gRPC Server]
запрос
<---
ответ


### Часть 2. Асинхронное взаимодействие (RabbitMQ + gRPC)

Брокер сообщений добавляется между producer и consumer, что позволяет:

- Гарантировать доставку сообщений (durable очередь).
- Масштабировать consumer независимо.
- Обрабатывать задачи без ожидания ответа.

**Диаграмма:**
[Producer] ---> [RabbitMQ] ---> [Consumer] ---> [gRPC Server]
(очередь)

**Логика работы компонентов:**

- **Producer** – публикует JSON‑задачи в очередь `task_queue`.
- **RabbitMQ** – хранит сообщения до момента их получения consumer'ом.
- **Consumer** – забирает сообщения, вызывает gRPC‑сервер, подтверждает обработку (ack).
- **gRPC Server** – выполняет задачу (регистрация, MIME, разбиение на предложения).

**Преимущества для моего варианта:**  
Задачи регистрации, определения MIME и разбиения текста не требуют синхронного ответа.  
Очередь позволяет накапливать запросы при временной недоступности gRPC‑сервера.

---

## Файл docker-compose.yml

```yaml
version: '3.8'
services:
  rabbitmq:
    image: rabbitmq:3.9-management
    container_name: rabbitmq
    ports:
      - "5672:5672"
      - "15672:15672"
    environment:
      RABBITMQ_DEFAULT_USER: user
      RABBITMQ_DEFAULT_PASS: password
    volumes:
      - rabbitmq_data:/var/lib/rabbitmq

volumes:
  rabbitmq_data:
```
**Логика работы компонентов:**

- **Producer** – публикует JSON‑задачи в очередь `task_queue`.
- **RabbitMQ** – хранит сообщения до момента их получения consumer'ом.
- **Consumer** – забирает сообщения, вызывает gRPC‑сервер, подтверждает обработку (ack).
- **gRPC Server** – выполняет задачу (регистрация, MIME, разбиение на предложения).

**Преимущества для моего варианта:**  
Задачи регистрации, определения MIME и разбиения текста не требуют синхронного ответа.  
Очередь позволяет накапливать запросы при временной недоступности gRPC‑сервера.

---

## Файл docker-compose.yml

```yaml
version: '3.8'
services:
  rabbitmq:
    image: rabbitmq:3.9-management
    container_name: rabbitmq
    ports:
      - "5672:5672"
      - "15672:15672"
    environment:
      RABBITMQ_DEFAULT_USER: user
      RABBITMQ_DEFAULT_PASS: password
```
**Запуск**
docker-compose up -d


## message_service.proto

```yaml
syntax = "proto3";

package message;

service MessageService {
  rpc ProcessMessage (MessageRequest) returns (MessageResponse) {}
}

message MessageRequest {
  string text = 1;
}

message MessageResponse {
  string processed_text = 1;
}


```
## grpc_server.py (фрагмент)

```yaml
class MessageService(...):
    def ProcessMessage(self, request, context):
        data = json.loads(request.text)
        task_type = data["task"]
        if task_type == "register":
            return MessageResponse(processed_text=f"Пользователь {email} зарегистрирован")
        elif task_type == "mime":
            return MessageResponse(processed_text=mime)
        elif task_type == "split_sentences":
            return MessageResponse(processed_text=json.dumps(sentences))

```
## producer.py

```yaml
tasks = {
    "register": {"task": "register", "payload": {"email": "test@example.com"}},
    "mime": {"task": "mime", "payload": {"filename": "photo.jpg"}},
    "split_sentences": {"task": "split_sentences", "payload": {"text": "Hello world! How are you? I am fine."}}
}
```
## consumer.py

```yaml
def callback(ch, method, properties, body):
    result = call_grpc(body.decode())
    ch.basic_ack(delivery_tag=method.delivery_tag)

```
# Результат выполнения: 

```yaml
[*] Ожидание сообщений. Нажмите CTRL+C для выхода
[x] Получено: {"task": "register", "payload": {"email": "test@example.com", "password": "123"}}
[✓] Результат: Пользователь test@example.com зарегистрирован
[x] Получено: {"task": "mime", "payload": {"filename": "photo.jpg"}}
[✓] Результат: image/jpeg
[x] Получено: {"task": "split_sentences", "payload": {"text": "Hello world! How are you? I am fine."}}
[✓] Результат: ["Hello world", "How are you", "I am fine"]

```
# Вывод: 
Разработанная система демонстрирует:

Синхронное взаимодействие через gRPC (часть 1).

Асинхронную обработку задач через брокер RabbitMQ (часть 2).

Полную независимость producer и consumer.

Успешную обработку трёх заданий варианта 11.
