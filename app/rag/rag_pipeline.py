from langchain_text_splitters import RecursiveCharacterTextSplitter
from app.api.google_driver import GoogleDriveLoader
from langchain_chroma import Chroma
from langchain_ollama import OllamaEmbeddings, ChatOllama
from langchain_openai import OpenAIEmbeddings, ChatOpenAI
from langchain_core.documents import Document
from typing import List


class RAGPipeline:
    def __init__(self, embedding_model: str = "ollama", llm_model: str = "ollama"):
        """
        Inicializa a pipeline RAG com modelos de embeddings e LLM especificados.
        
        :param embedding_model: Nome do modelo de embeddings ("ollama" ou "openai").
        :param llm_model: Nome do modelo LLM ("ollama" ou "openai").
        """
        self.embedding_model = embedding_model
        self.llm_model = llm_model
        self.embeddings = self._get_embeddings()
        self.llm = self._get_llm()
        
    def get_documents_from_drive(self, folder_id: str) -> List[str]:
        """
        Recupera documentos de um folder no Google Drive usando o GoogleDriveLoader.
        
        :param folder_id: O ID da pasta no Google Drive.
        :return: Lista de documentos encontrados.
        """
        loader = GoogleDriveLoader(folder_id)
        docs = loader.load() 
        print(f"Quantidade de documentos: {len(docs)}")
        for i, doc in enumerate(docs):
            print(f"Documento {i}: Tipo do documento: {type(doc)}")
        return [doc for doc in docs if isinstance(doc, Document)]

    def _get_embeddings(self):
        """
        Configura o modelo de embeddings com base no nome especificado.
        """
        if self.embedding_model == "ollama::nomic-embed-text":
            return OllamaEmbeddings(model="nomic-embed-text")
        
        elif self.embedding_model == "openai::text-embedding-ada-002":
            return OpenAIEmbeddings(model="text-embedding-ada-002")
        else:
            raise ValueError(f"Modelo de embeddings '{self.embedding_model}' não reconhecido!")

    def _get_llm(self):
        """
        Configura o modelo de LLM com base no nome especificado.
        """
        if self.llm_model == "llama3.1:8b":
            return ChatOllama(model="llama3.1:8b")
        elif self.llm_model == "llama3.1:70b":
            return ChatOllama(model="llama3.1:70b")
        elif self.llm_model == "gpt-4":
            return ChatOpenAI(model="gpt-4")
    
        else:
            raise ValueError(f"Modelo de LLM '{self.llm_model}' não reconhecido!")

    def prepare_documents(self, docs: List[str]):
        """
        Divide os documentos em chunks menores utilizando RecursiveCharacterTextSplitter.
        
        :param docs: Lista de strings representando os documentos.
        :return: Lista de documentos divididos.
        """
        text_splitter = RecursiveCharacterTextSplitter(chunk_size=500, chunk_overlap=0)
        return text_splitter.split_documents(docs)

    def create_vectorstore(self, docs: List[str]):
        """
        Cria um vetor de recuperação (vectorstore) usando Chroma.
        
        :param docs: Lista de strings representando os documentos.
        :return: Vectorstore configurado.
        """
        all_splits = self.prepare_documents(docs)
        return Chroma.from_documents(documents=all_splits, embedding=self.embeddings)

    def query(self, vectorstore, query: str) -> str:
        """
        Faz uma consulta ao LLM utilizando o vetor de recuperação e busca documentos relevantes no vectorstore.
        """
        import time
        
        start_time = time.time()
        relevant_docs = vectorstore.similarity_search(query, k=5) 
        print(f"Tempo para similarity_search: {time.time() - start_time:.2f}s")
        
        context = "\n\n".join([doc.page_content for doc in relevant_docs])
        max_context_length = 1000 
        truncated_context = context[:max_context_length]
        
        start_time = time.time()
        response = self.llm.invoke(
            input=f"Contexto:\n{truncated_context}\n\nPergunta: {query}",
            max_tokens=500, 
            temperature=0.7,
            top_p=0.9
        )
        print(f"Tempo para consulta ao LLM: {time.time() - start_time:.2f}s")
        
        return response.content


