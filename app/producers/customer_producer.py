from app.kafka.producer import KafkaProducer

class CustomerProducer(KafkaProducer):
    def __init__(self, broker_url, client_id):
        # Inicializa a classe base com os parâmetros necessários
        super().__init__(broker_url=broker_url, client_id=client_id, name="Customer")

    async def send_folder_id(self, folder_id, customer_id):
        message = {
            "event_type": "customer-created.reply",
            "data": {
                "folder_id": folder_id,
                "customer_id": customer_id
            }
        }
        await self.send_message("customer-created.reply", message)
