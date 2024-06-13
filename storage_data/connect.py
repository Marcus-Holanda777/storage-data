from google.cloud.storage import (
    Client, 
    Bucket,
    Blob
)
from typing import Any, Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from functools import partial

from enum import Enum
from os import cpu_count


class StorageClass(Enum):
    STANDARD = 0
    NEARLINE = 1
    CODLINE = 2
    ARCHIVE = 3


DEFAULT_CLASS = StorageClass.STANDARD
WORKERS = min([8, cpu_count()])
MEGA_BYTES = 1 << 20


class SConnect:
    def __init__(
        self, 
        credential: str = None
    ) -> None:  
        self.credential = credential
    
    def __call__(self, *args: Any, **kwds: Any) -> Client:
        for k, v in kwds.items():
            setattr(self, k, v)
        
        if self.credential:
            return Client.from_service_account_json(self.credential)
        
        return Client(*args, **kwds)
    

class Storage:
    def __init__(self, cliente: Client) -> None:
        self.cliente = cliente

    def create_bucket(
        self, 
        bucket_name: str,
        storage_class: StorageClass = DEFAULT_CLASS,
        location: str = 'us-east1'
    ) -> Bucket:
        
        """Criar bucket GCS.

        Método para criar um bucket no `google-cloud-storage`.

        Parâmetros
        ----------
        bucket_name: str
            Nome do bucket que será criado.

        storage_class: StorageClass
            Define a classe de armazenamento.

        - `STANDART`
        - `NEARLINE`
        - `CODLINE`
        - `ARCHIVE`

        location: str
            Define a região onde será criado o bucket, ex: `US-EAST1`, 
            `US-SOUTH1`
            
        Retorno
        -------
            Objeto do tipo `Bucket`

        """
        
        bucket = self.cliente.bucket(bucket_name)
        bucket.storage_class = storage_class.name
        bucket.location = location

        return self.cliente.create_bucket(bucket)
    
    def get_bucket(
        self, 
        bucket: Bucket | str
    ) -> Bucket:
        return self.cliente.get_bucket(bucket)
    
    def list_name_bucket(self) -> Iterator[str]:
        return (
            bk.name
            for bk in self.cliente.list_buckets()
        )
    
    def upload_file(
        self, 
        bucket: Bucket,
        source_file_name: str,
        destination_blob_name: str,
        timeout: int = 180
    ) -> Blob | None:
        
        try:
            blob = bucket.blob(destination_blob_name)

            blob.upload_from_filename(
                source_file_name,
                timeout=timeout
            )

            print(f'File - {destination_blob_name} .. OK')
    
        except Exception as e:
            print(f'Reset: {destination_blob_name} .. ERROR {e}')
            self.upload_file(bucket, source_file_name, destination_blob_name)
            
        return blob
    
    def upload_files(
        self,
        bucket: Bucket,
        path: Path | str,
        pattern: str,
        *,
        prefix_blob: str = None,
        sub_path: str = None,
        index_sub: int = 4,
        max_workers: int = WORKERS
    ) -> Iterator[Blob | None]:
        
        if isinstance(path, str):
            path = Path(path)
        
        def blob_source_dest(file_path: Path):
            if prefix_blob:
                return str(file_path), prefix_blob
            
            if sub_path and index_sub > 0:
                blob_paths = '/'.join(file_path.parts[-index_sub:])
                return str(file_path), f'{sub_path}/{blob_paths}'
            
            return str(file_path), file_path.name
    
        def up_filename_name(args):
            source, destinetion = args

            up_file = partial(
                self.upload_file,
                bucket
            )

            return up_file(source, destinetion)

        list_map_bob_destino = map(
            blob_source_dest, 
            path.glob(pattern)
        )
               
        with ThreadPoolExecutor(
            max_workers=max_workers
        ) as executor:
            
            rst = executor.map(
                up_filename_name,
                list_map_bob_destino
            )

        return rst
    
    def list_files(
        self,
        bucket: Bucket | str,
        prefix: str = None,
        delimiter: str = None
    ) -> Iterator[Blob | None]:
        
        blobs = (
            self.cliente
             .list_blobs(
                 bucket,
                 prefix=prefix, 
                 delimiter=delimiter
            )
        )
        
        for blob in blobs:
            yield blob

    def download_file(
        self,
        bucket: Bucket, 
        source_blob_name: str, 
        destination_file_name: str
    ) -> Blob | None:
        
        try:
           
           blob = bucket.blob(source_blob_name)
           blob.download_to_filename(destination_file_name)
           size_file = blob.size / MEGA_BYTES

        except Exception as e:
            print(f'File: {destination_file_name} .. ERROR {e}')
        else:
            print(f'File - {destination_file_name}, {size_file:.2f} MB .. OK')

        return blob