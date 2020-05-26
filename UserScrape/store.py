from azure.storage.blob import ContainerClient
from pathlib import Path, PurePath
import tempfile
import json
import os
from typing import Union
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import PublicAccess


class BlobStore:
    def __init__(self, cs: str, containerName: str):
        self.cs = cs
        self.container = ContainerClient.from_connection_string(
            cs, containerName)

    def ensure_exits(self, public_access: PublicAccess = None):
        """creates the container if it doesn't exist"""
        try:
            props = self.container.get_container_properties()
        except ResourceNotFoundError:
            self.container.create_container(public_access=public_access)
        except BaseException as e:
            raise e

    def save(self, path: PurePath, content: Union[str, dict]):
        """Saves text content to a blob with the given path. W

        Arguments:
            relativePath {PurePath} -- a path within this container
            content {Union[str, dict]} -- str content will be saved as is, dict will be serialized to json
        """
        txt = json.dumps(content) if isinstance(content, dict) else content

        localPath = Path(tempfile.gettempdir()) / path
        localPath.parent.mkdir(parents=True, exist_ok=True)
        with open(localPath, "w", encoding="utf-8") as w:
            w.write(txt)
        self.save_file(localPath, path)
        os.remove(localPath)

    def save_file(self, localFile: PurePath, remotePath: PurePath):
        """uploads a local file to the container"""
        with open(localFile, 'rb') as f:
            self.container.upload_blob(
                remotePath.as_posix(), f, overwrite=True)

    def load(self, path: PurePath):
        try:
            blob = self.container.download_blob(path.as_posix())
        except BaseException:
            return None
        return blob.content_as_text()

    def load_dic(self, path: PurePath):
        txt = self.load(path)
        return json.loads(txt) if txt else None
