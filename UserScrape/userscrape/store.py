from azure.storage.blob import ContainerClient, BlobProperties
from pathlib import Path, PurePath
import tempfile
import json
import os
from typing import Union, Iterator
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import PublicAccess, BlobProperties
from azure.core.paging import ItemPaged
from datetime import datetime
import shortuuid
from .cfg import UserCfg, StoreCfg
from pathlib import Path, PurePath, PurePosixPath
import gzip
from io import BytesIO
from azure.storage.blob._models import ContentSettings


class BlobStore:
    def __init__(self, cfg: StoreCfg):
        self.cfg = cfg
        self.container = ContainerClient.from_connection_string(cfg.cs, cfg.container)

    def url(self, path: PurePath):
        blob = self.container.get_blob_client(path.as_posix())
        return blob.url

    def ensure_container_exits(self, public_access: PublicAccess = None):
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

    def save_file(self, localFile: PurePath, remotePath: PurePath, content_type: str = None):
        """uploads a local file to the container"""
        with open(localFile, 'rb') as f:
            blob = self.container.get_blob_client(remotePath.as_posix())
            blob.upload_blob(f,
                             overwrite=True,
                             content_settings=ContentSettings(content_type=content_type) if content_type else None)

    def load(self, path: PurePath) -> str:
        try:
            blob = self.container.download_blob(path.as_posix())
        except ResourceNotFoundError:
            return None
        return blob.content_as_text()

    def load_file(self, local_file: PurePath, remote_file: PurePath):
        """loads a blob into a local file"""
        with open(local_file, 'wb') as w:
            blob = self.container.get_blob_client(remote_file.as_posix())
            props: BlobProperties = blob.get_blob_properties()

            if(props.content_settings.content_encoding == 'gzip'):  # ffs https://github.com/Azure/azure-storage-python/issues/548
                dl = self.container.download_blob(remote_file.as_posix())
                content = dl.content_as_bytes()
                data_gz = gzip.compress(content)
                w.write(data_gz)
            else:
                dl = self.container.download_blob(remote_file.as_posix())
                dl.readinto(w)

    def load_dic(self, path: PurePath):
        txt = self.load(path)
        return json.loads(txt) if txt else None

    def exists(self, path: PurePath):
        try:
            blob = self.container.get_blob_client(path.as_posix())
            blob.get_blob_properties()
            return True
        except ResourceNotFoundError:
            return False

    def list(self, starts_with: str = None) -> Iterator[BlobProperties]:
        return self.container.list_blobs(starts_with)


def new_trial_id():
    return f'{file_date_str()}_{shortuuid.random(3)}'


def file_date_str(time: datetime = datetime.now()):
    return time.strftime('%Y-%m-%d_%H-%M-%S')


class BlobPaths:
    def __init__(self, storeCfg: StoreCfg, trial_id: str, user: UserCfg = None, session_id: str = None):
        self.storeCfg = storeCfg
        self.trial_id = trial_id
        self.user = user
        self.session_id = session_id

    def results_path_recfluence(self) -> PurePath:
        """path that recfluence stores its latest results"""
        return PurePosixPath('results')

    def results_path_out(self) -> PurePath:
        """path to store the results of trials once complete"""
        return PurePosixPath(f'{self.storeCfg.root_path}/results')

    def results_path_in(self) -> PurePath:
        """path to incoming files (e.g. video seeds from recfluence)"""
        return self.__trial_path('results_in')

    def __trial_path(self, catalog: str) -> PurePath:
        return PurePosixPath(f'{self.storeCfg.root_path}/run/{catalog}/{self.trial_id}')

    def session_path(self) -> PurePath:
        return PurePosixPath(f'{self.storeCfg.root_path}/run/session_logs/{self.trial_id}/{self.user.tag}/{self.session_id}')

    def user_path(self) -> PurePath:
        return PurePosixPath(f'{self.storeCfg.root_path}/run/user/{self.user.tag}')

    def __trial_user_path(self, catalog: str) -> PurePath:
        return self.__trial_path(catalog) / self.user.tag

    def __trial_video(self, catalog: str, video_id: str) -> PurePath:
        return PurePosixPath(f'{self.__trial_user_path(catalog)}_{video_id}.json')

    def cookies_json(self) -> PurePath:
        return self.user_path() / 'cookies.json'

    def trial_cfg_json(self) -> PurePath:
        return self.__trial_path("cfg") / 'cfg.json'

    def rec_path(self) -> PurePath:
        return self.__trial_path("recommendations")

    def rec_json(self, video_id) -> PurePath:
        return self.__trial_video("recommendations", video_id)

    def ad_path(self) -> PurePath:
        return self.__trial_path("advertisements")

    def ad_json(self, video_id) -> PurePath:
        return self.__trial_video("advertisements", video_id)

    def watch_time_json(self, video_id: str) -> PurePath:
        return self.__trial_video("watch_times", video_id)

    def watch_time_path(self) -> PurePath:
        return self.__trial_path("watch_times")

    def feed_json(self, scan_num: int) -> PurePath:
        return PurePosixPath(f'{self.__trial_user_path("feed")}.{scan_num}.json')

    def feed_path(self) -> PurePath:
        return self.__trial_path("feed")

    def local_temp_path(self, path: PurePath) -> Path:
        return Path(tempfile.gettempdir()) / 'userscrape' / self.trial_id / path
