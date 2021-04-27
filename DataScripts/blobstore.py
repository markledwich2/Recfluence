from cfg import StoreCfg
from azure.storage.blob._models import ContentSettings
from azure.storage.blob import ContainerClient, PublicAccess
from azure.core.exceptions import ResourceNotFoundError
from pathlib import PurePath


class BlobStore:
    def __init__(self, cfg: StoreCfg):
        self.cfg = cfg
        self.container = ContainerClient.from_connection_string(cfg.dataStorageCs, cfg.container)

    def ensure_container_exits(self, public_access: PublicAccess = None):
        """creates the container if it doesn't exist"""
        try:
            props = self.container.get_container_properties()
        except ResourceNotFoundError:
            self.container.create_container(public_access=public_access)
        except BaseException as e:
            raise e

    def save_file(self, localFile: PurePath, remotePath: PurePath, content_type: str = None):
        """uploads a local file to the container"""
        with open(localFile, 'rb') as f:
            blob = self.container.get_blob_client(remotePath.as_posix())
            blob.upload_blob(f,
                             overwrite=True,
                             content_settings=ContentSettings(content_type=content_type) if content_type else None)

    def delete(self, path: PurePath):
        self.container.delete_blob(path.as_posix())
