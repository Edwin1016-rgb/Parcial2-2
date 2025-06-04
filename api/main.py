from fastapi import FastAPI, Request, HTTPException, Depends, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import aio_pika
import json
import base64

app = FastAPI()
security = HTTPBasic()

RABBITMQ_URL = "amqp://guest:guest@rabbitmq:5672/"
QUEUE_NAME = "messages"
USERNAME = "admin"
PASSWORD = "admin"

async def publish_message(message: dict):
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(QUEUE_NAME, durable=True)
        await channel.default_exchange.publish(
            aio_pika.Message(body=json.dumps(message).encode()),
            routing_key=queue.name
        )

def verify_credentials(credentials: HTTPBasicCredentials = Depends(security)):
    if credentials.username != USERNAME or credentials.password != PASSWORD:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")

@app.post("/api/message")
async def send_message(request: Request, creds: HTTPBasicCredentials = Depends(verify_credentials)):
    data = await request.json()
    await publish_message(data)
    return {"status": "message sent"}


@app.get("/health")
async def healthcheck():
    try:
        connection = await aio_pika.connect_robust(RABBITMQ_URL)
        async with connection:
            return {"status": "healthy", "message": "API is running and RabbitMQ is accessible."}
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="RabbitMQ is not accessible")

