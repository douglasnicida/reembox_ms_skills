from fastapi import FastAPI, WebSocket
from contextlib import asynccontextmanager
import asyncio
from app.db import database
from app.api.google_driver import GoogleDriveLoader
from app.producers.customer_producer import CustomerProducer
from app.consumers.customer_consumer import CustomerConsumer
from app.config import KAFKA_BROKER_URL
from app.rag.rag_pipeline import RAGPipeline

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Iniciando ciclo de vida do microsserviço...")

    # Conectar ao banco de dados
    await database.connect()

    # Instanciando o produtor Kafka
    producer = CustomerProducer(
        broker_url=KAFKA_BROKER_URL, 
        client_id="reembox"
    )

    # Inicializando o produtor Kafka
    await producer.initialize()

    # Instanciando o consumidor Kafka
    consumer = CustomerConsumer(
        producer=producer, 
        broker_url=KAFKA_BROKER_URL, 
        group_id="fastapi-consumer-client", 
        client_id="reembox"
    )

    # Inicializando o consumidor Kafka
    await consumer.initialize()

    # Inicia o loop de consumo de eventos em segundo plano
    event_consumer_task = asyncio.create_task(consumer.consume_events())

    yield  # O aplicativo está agora em execução

    print("Desligando ciclo de vida do microsserviço...")

    # Cancelando a tarefa do consumidor quando o aplicativo for desligado
    await consumer.cancel()

    # Parando o produtor Kafka ao final
    await producer.stop()

    # Desconectar do banco de dados
    await database.disconnect()

    # Aguardar o término do consumo
    await event_consumer_task

app = FastAPI(lifespan=lifespan)

loader = GoogleDriveLoader()

@app.get("/")
async def read_root():
    return {"message": "Microsserviço para gerenciar os chats de clientes"}

@app.websocket("/query/")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    while True:
        data = await websocket.receive_json()  # Espera um JSON com query e folderId
        query = data.get('query')
        folder_id = data.get('folderId')
        llmModel = data.get('llmModel')
        embeddingModel = data.get('embeddingModel')
        
        print(query, folder_id, llmModel, embeddingModel)

        if query and folder_id and llmModel and embeddingModel:
            rag_pipeline = RAGPipeline(embedding_model=embeddingModel,llm_model=llmModel)
                        
            import time

            start_time = time.time()
            docs = rag_pipeline.get_documents_from_drive(folder_id)
            print(f"Carregamento dos documentos: {time.time() - start_time:.2f}s")

            start_time = time.time()
            vectorstore = rag_pipeline.create_vectorstore(docs)
            print(f"Criação do vectorstore: {time.time() - start_time:.2f}s")

            start_time = time.time()
            response = rag_pipeline.query(vectorstore, query)
            print(f"Consulta ao modelo: {time.time() - start_time:.2f}s")

            # Enviar a resposta de volta
            await websocket.send_text(response)
        else:
            await websocket.send_text("Por favor, forneça tanto 'query' quanto 'folderId'.")
