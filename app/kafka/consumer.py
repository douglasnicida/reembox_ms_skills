from aiokafka import AIOKafkaConsumer
import json


class KafkaEventConsumer:
    def __init__(self, topic, broker_url, group_id, client_id):
        self.topic = topic
        self.broker_url = broker_url
        self.group_id = group_id
        self.client_id = client_id
        self.consumer = None

    async def initialize(self):
        """Inicializa o consumidor Kafka."""
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.broker_url,
            group_id=self.group_id,
            client_id=self.client_id,
            auto_offset_reset="earliest",  # Garantir que o consumidor leia desde o início
        )
        print(f"Consumidor Kafka para o tópico {self.topic} iniciado.")
        await self.consumer.start()

    async def consume_events(self):
        """Consumir eventos do Kafka e processar."""
        print(f"Iniciando o consumo de eventos do tópico: {self.topic}")
        try:
            # Usando async for para consumir mensagens de forma eficiente
            async for msg in self.consumer:
                message = json.loads(msg.value.decode('utf-8'))
                print(f"Mensagem recebida: {message}")
                await self.handle_event(message)

                # Commit do offset após processar a mensagem
                await self.consumer.commit()

        except Exception as e:
            print(f"Erro durante o consumo: {e}")
        finally:
            await self.consumer.stop()
            print("Consumidor parado.")

    async def handle_event(self, message):
        """Processa os eventos conforme o tipo. Deve ser sobrescrito pelas subclasses."""
        raise NotImplementedError("Este método deve ser implementado pela subclasse.")
