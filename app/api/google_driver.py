import os
from urllib.error import HTTPError

from google.oauth2 import service_account
from googleapiclient.discovery import build
from langchain.document_loaders.base import BaseLoader
from langchain_core.documents import Document
from app.config import *

class GoogleDriveLoader(BaseLoader):
    def __init__(self, folder_id: str = ""):
        self.parent_folder_id = PARENT_FOLDER_ID
        self.service_account_file = GOOGLE_APPLICATION_CREDENTIALS
        self.creds = self.authenticate()
        
        self.folder_id = folder_id
        
        print(f"{GOOGLE_APPLICATION_CREDENTIALS} - {PARENT_FOLDER_ID}")

    def authenticate(self):
        SCOPES = [
            # Permite acesso a arquivos criados ou abertos por sua aplicação
            'https://www.googleapis.com/auth/drive.file',
            # Acesso total ao Google Drive
            'https://www.googleapis.com/auth/drive'        
        ]
        
        # Cria as credenciais a partir do arquivo da conta de serviço
        creds = service_account.Credentials.from_service_account_file(
            self.service_account_file, scopes=SCOPES)
        return creds

    def load(self):
        service = build('drive', 'v3', credentials=self.creds)
        return self.list_docs_in_folder(service, self.folder_id)

    def list_docs_in_folder(self, service, folder_id):
        docs = []
        query = f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.document'"
        
        results = service.files().list(q=query, fields="files(id, name)").execute()
        items = results.get('files', [])
        
        for item in items:
            content = self.get_document_content(item['id'])
            docs.append(Document(page_content=content, metadata={"id": item["id"], "name": item["name"]}))
        
        query_folders = f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.folder'"
        folder_results = service.files().list(q=query_folders, fields="files(id, name)").execute()
        
        folders = folder_results.get('files', [])
        
        for folder in folders:
            docs.extend(self.list_docs_in_folder(service, folder['id']))
        
        return docs

    def get_document_content(self, document_id):
        service = build('docs', 'v1', credentials=self.creds)
        doc = service.documents().get(documentId=document_id).execute()
        
        content = ''
        for element in doc.get('body').get('content'):
            if 'paragraph' in element:
                for text_run in element['paragraph'].get('elements'):
                    if 'textRun' in text_run:
                        content += text_run['textRun']['content']
        
        return content
      
    def create_folder(self, folder_name, company):
      service = build('drive', 'v3', credentials=self.creds)
      file_metadata = {
          'name': folder_name,
          'mimeType': 'application/vnd.google-apps.folder',
          'parents': [self.parent_folder_id]
      }
      
      try:
          file = service.files().create(body=file_metadata, fields='id').execute()
          return file.get('id')
      except HTTPError as error:
          print(f'Ocorreu um erro ao criar a pasta para a empresa {company}: {error}')
          return None
