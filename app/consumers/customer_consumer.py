from app.api.google_driver import GoogleDriveLoader
from app.kafka.consumer import KafkaEventConsumer
from app.db import database

class CustomerConsumer(KafkaEventConsumer):
    def __init__(self, producer, broker_url, group_id, client_id):
        super().__init__(topic="customer-created", broker_url=broker_url, group_id=group_id, client_id=client_id)
        self.producer = producer

    async def handle_event(self, message):
        """Processa os eventos conforme o tipo."""
        event_type = message.get("event_type")
        if event_type == "customer-created":
            await self.handle_create_customer(message["data"])
        else:
            print(f"Tipo de evento desconhecido: {event_type}")

    async def handle_create_customer(self, data):
        """Processa o evento de criação de cliente."""
        print(f"Data: {data}, Tipo: {type(data)}")
        folder_name = f"{data.get('id')}-{data.get('name')}_{data.get('createdAt')}"

        # Cria a pasta no Google Drive
        loader = GoogleDriveLoader()
        folder_id = loader.create_folder(folder_name, data.get("name"))

        query = """
        UPDATE "Customer"
        SET "folderId" = :folder_id
        WHERE "id" = :customer_id
        """
        values = {"folder_id": folder_id, "customer_id": data.get("id")}
        await database.execute(query=query, values=values)