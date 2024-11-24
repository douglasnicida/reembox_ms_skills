from aiokafka import AIOKafkaProducer
import json

class KafkaProducer:
    def __init__(self, broker_url, client_id, name):
        self.name = name
        self.broker_url = broker_url
        self.client_id = client_id
        self.producer = None

    async def initialize(self,):
        """Inicializa o produtor Kafka."""
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.broker_url,
            client_id=self.client_id
        )
        await self.producer.start()
        print(f"Produtor Kafka iniciado para o serviço {self.name}.")

    async def send_message(self, topic, message):
        """Envia uma mensagem para o Kafka."""
        try:
            await self.producer.send_and_wait(topic, json.dumps(message).encode('utf-8'))
            print(f"Mensagem enviada para o Kafka: {message}")
        except Exception as e:
            print(f"Erro ao enviar mensagem para o Kafka: {e}")

    async def stop(self):
        """Encerra o produtor Kafka."""
        if self.producer:
            await self.producer.stop()
            print("Produtor Kafka parado.")
