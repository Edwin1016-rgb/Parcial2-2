import asyncio
import aio_pika
import json
import os

RABBITMQ_URL = "amqp://guest:guest@rabbitmq:5672/"
QUEUE_NAME = "messages"
LOG_FILE_PATH = "/data/messages.log"

async def connect_with_retry(url, retries=10, delay=5):
    for attempt in range(retries):
        try:
            return await aio_pika.connect_robust(url)
        except Exception as e:
            print(f"❌ Conexión fallida a RabbitMQ (intento {attempt + 1}/{retries}): {e}")
            await asyncio.sleep(delay * (attempt + 1))
    raise RuntimeError("No se pudo conectar a RabbitMQ tras varios intentos.")

async def handle_message(message: aio_pika.IncomingMessage):
    async with message.process():
        body = message.body.decode()
        print(f"✅ Mensaje recibido: {body}")
        with open(LOG_FILE_PATH, "a") as f:
            f.write(body + "\n")

async def main():
    os.makedirs("/data", exist_ok=True)
    connection = await connect_with_retry(RABBITMQ_URL)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)
    queue = await channel.declare_queue(QUEUE_NAME, durable=True)
    await queue.consume(handle_message)
    print("🎧 Escuchando mensajes...")
    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
