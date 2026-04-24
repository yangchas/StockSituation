import pika
import os

try:
    params = pika.URLParameters('amqp://admin:admin@localhost:5672/')
    conn = pika.BlockingConnection(params)
    channel = conn.channel()
    count = channel.queue_purge('stream2')
    print(f'SUCCESS: Purged {count} messages from stream2')
    conn.close()
except Exception as e:
    print(f'ERROR: {e}')
