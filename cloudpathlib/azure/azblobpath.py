import os
from pathlib import Path, PosixPath, WindowsPath
from tempfile import TemporaryDirectory
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    BinaryIO,
    Literal,
    Self,
    overload,
)
from io import BufferedRandom, BufferedReader, BufferedWriter, FileIO, TextIOWrapper

from cloudpathlib.exceptions import CloudPathFileNotFoundError, CloudPathIsADirectoryError

try:
    from azure.core.exceptions import ResourceNotFoundError
except ImportError:
    pass

from .. import anypath
from ..cloudpath import (
    CloudPath,
    CloudPathFileExistsError,
    CloudPathIsADirectoryError,
    CloudPathNotExistsError,
    FileCacheMode,
    NoStatError,
    OverwriteDirtyFileError,
    OverwriteNewerCloudError,
    OverwriteNewerLocalError,
    register_path_class,
)


if TYPE_CHECKING:
    from .azblobclient import AzureBlobClient, AzurePathProperties
    from _typeshed import (
        OpenBinaryMode,
        OpenBinaryModeReading,
        OpenBinaryModeUpdating,
        OpenBinaryModeWriting,
        OpenTextMode,
    )


@register_path_class("azure")
class AzureBlobPath(CloudPath):
    """Class for representing and operating on Azure Blob Storage URIs, in the style of the Python
    standard library's [`pathlib` module](https://docs.python.org/3/library/pathlib.html).
    Instances represent a path in Blob Storage with filesystem path semantics, and convenient
    methods allow for basic operations like joining, reading, writing, iterating over contents,
    etc. This class almost entirely mimics the [`pathlib.Path`](https://docs.python.org/3/library/pathlib.html#pathlib.Path)
    interface, so most familiar properties and methods should be available and behave in the
    expected way.

    The [`AzureBlobClient`](../azblobclient/) class handles authentication with Azure. If a
    client instance is not explicitly specified on `AzureBlobPath` instantiation, a default client
    is used. See `AzureBlobClient`'s documentation for more details.
    """

    cloud_prefix: str = "az://"
    client: "AzureBlobClient"

    @property
    def drive(self) -> str:
        return self.container

    def mkdir(self, parents=False, exist_ok=False):
        self.client._mkdir(self, parents=parents, exist_ok=exist_ok)

    def touch(self, exist_ok: bool = True):
        if self.exists():
            if not exist_ok:
                raise FileExistsError(f"File exists: {self}")
            self.client._move_file(self, self)
        else:
            tf = TemporaryDirectory()
            p = Path(tf.name) / "empty"
            p.touch()

            self.client._upload_file(p, self)

            tf.cleanup()

    def replace(self, target: "AzureBlobPath") -> "AzureBlobPath":
        try:
            return super().replace(target)

        # we can rename directories on ADLS Gen2
        except CloudPathIsADirectoryError:
            if self.client._check_hns(self):
                return self.client._move_file(self, target)
            else:
                raise

    @property
    def container(self) -> str:
        return self._no_prefix.split("/", 1)[0]

    @property
    def blob(self) -> str:
        key = self._no_prefix_no_drive

        # key should never have starting slash for
        if key.startswith("/"):
            key = key[1:]

        return key

    @property
    def etag(self):
        return self.client._get_metadata(self).get("etag", None)

    @property
    def md5(self) -> str:
        return self.client._get_metadata(self).get("content_settings", {}).get("content_md5", None)

    def __fspath__(self) -> str:
        meta = self.client._get_path_meta(self)
        if not meta.is_directory:
            self._refresh_cache_with_meta(meta)
        return str(self._local)

    @overload
    def open(
        self,
        mode: "OpenTextMode" = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
        force_overwrite_from_cloud: bool | None = None,
        force_overwrite_to_cloud: bool | None = None,
    ) -> TextIOWrapper: ...

    @overload
    def open(
        self,
        mode: "OpenBinaryMode",
        buffering: Literal[0],
        encoding: None = None,
        errors: None = None,
        newline: None = None,
        force_overwrite_from_cloud: bool | None = None,
        force_overwrite_to_cloud: bool | None = None,
    ) -> FileIO: ...

    @overload
    def open(
        self,
        mode: "OpenBinaryModeUpdating",
        buffering: Literal[-1, 1] = -1,
        encoding: None = None,
        errors: None = None,
        newline: None = None,
        force_overwrite_from_cloud: bool | None = None,
        force_overwrite_to_cloud: bool | None = None,
    ) -> BufferedRandom: ...

    @overload
    def open(
        self,
        mode: "OpenBinaryModeWriting",
        buffering: Literal[-1, 1] = -1,
        encoding: None = None,
        errors: None = None,
        newline: None = None,
        force_overwrite_from_cloud: bool | None = None,
        force_overwrite_to_cloud: bool | None = None,
    ) -> BufferedWriter: ...

    @overload
    def open(
        self,
        mode: "OpenBinaryModeReading",
        buffering: Literal[-1, 1] = -1,
        encoding: None = None,
        errors: None = None,
        newline: None = None,
        force_overwrite_from_cloud: bool | None = None,
        force_overwrite_to_cloud: bool | None = None,
    ) -> BufferedReader: ...

    @overload
    def open(
        self,
        mode: "OpenBinaryMode",
        buffering: int = -1,
        encoding: None = None,
        errors: None = None,
        newline: None = None,
        force_overwrite_from_cloud: bool | None = None,
        force_overwrite_to_cloud: bool | None = None,
    ) -> BinaryIO: ...

    @overload
    def open(
        self,
        mode: str,
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
        force_overwrite_from_cloud: bool | None = None,
        force_overwrite_to_cloud: bool | None = None,
    ) -> IO[Any]: ...

    def open(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
        force_overwrite_from_cloud: bool | None = None,  # extra kwarg not in pathlib
        force_overwrite_to_cloud: bool | None = None,  # extra kwarg not in pathlib
    ) -> IO[Any]:
        meta = self.client._get_path_meta(self)
        # if trying to call open on a directory that exists
        if meta.exists and meta.is_directory:
            raise CloudPathIsADirectoryError(
                f"Cannot open directory, only files. Tried to open ({self})"
            )

        if not meta.exists and any(m in mode for m in ("r", "a")):
            raise CloudPathFileNotFoundError(
                f"File opened for read or append, but it does not exist on cloud: {self}"
            )

        if mode == "x" and meta.exists:
            raise CloudPathFileExistsError(f"Cannot open existing file ({self}) for creation.")

        # TODO: consider streaming from client rather than DLing entire file to cache
        self._refresh_cache_with_meta(
            meta=meta, force_overwrite_from_cloud=force_overwrite_from_cloud
        )

        # create any directories that may be needed if the file is new
        if not self._local.exists():
            self._local.parent.mkdir(parents=True, exist_ok=True)
            original_mtime = 0.0
        else:
            original_mtime = self._local.stat().st_mtime

        buffer = self._local.open(
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )

        # write modes need special on closing the buffer
        if any(m in mode for m in ("w", "+", "x", "a")):
            # dirty, handle, patch close
            wrapped_close = buffer.close

            # since we are pretending this is a cloud file, upload it to the cloud
            # when the buffer is closed
            def _patched_close_upload(*args: Any, **kwargs: Any) -> None:
                wrapped_close(*args, **kwargs)

                # we should be idempotent and not upload again if
                # we already ran our close method patch
                if not self._dirty:
                    return

                # original mtime should match what was in the cloud; because of system clocks or rounding
                # by the cloud provider, the new version in our cache is "older" than the original version;
                # explicitly set the new modified time to be after the original modified time.
                if self._local.stat().st_mtime < original_mtime:
                    new_mtime = original_mtime + 1
                    os.utime(self._local, times=(new_mtime, new_mtime))

                self._upload_local_to_cloud_with_meta(
                    meta, force_overwrite_to_cloud=force_overwrite_to_cloud
                )
                self._dirty = False

            buffer.close = _patched_close_upload  # type: ignore

            # keep reference in case we need to close when __del__ is called on this object
            self._handle = buffer

            # opened for write, so mark dirty
            self._dirty = True

        # if we don't want any cache around, remove the cache
        # as soon as the file is closed
        if self.client.file_cache_mode == FileCacheMode.close_file:
            # this may be _patched_close_upload, in which case we need to
            # make sure to call that first so the file gets uploaded
            wrapped_close_for_cache = buffer.close

            def _patched_close_empty_cache(*args: Any, **kwargs: Any) -> None:
                wrapped_close_for_cache(*args, **kwargs)

                # remove local file as last step on closing
                self.clear_cache()  # type: ignore

            buffer.close = _patched_close_empty_cache  # type: ignore

        return buffer


    def _stat_with_meta(self, meta: "AzurePathProperties") -> os.stat_result:
        if not meta.exists:
            raise NoStatError(
                f"No stats available for {self}; it may be a directory or not exist."
            )

        return os.stat_result(
            (
                0,  # mode
                0,  # ino
                0,  # dev,
                0,  # nlink,
                0,  # uid,
                0,  # gid,
                meta.size if meta.size is not None else 0,  # size,
                0,  # atime,
                meta.last_modified.timestamp() if meta.last_modified is not None else 0,  # mtime,
                0,  # ctime,
            )
        )


    def stat(self, _: bool = True) -> os.stat_result:
        meta = self.client._get_path_meta(self)
        return self._stat_with_meta(meta)


    # ===========  public cloud methods, not in pathlib ===============
    def _download_to_with_meta(
        self, meta: "AzurePathProperties", destination: str | os.PathLike[Any]
    ) -> Path:
        destination = Path(destination)

        if not meta.exists:
            raise CloudPathNotExistsError(f"Cannot download because path does not exist: {self}")

        if not meta.is_directory:
            if destination.is_dir():
                destination = destination / self.name
            return self.client._download_file(self, destination)

        # TODO: not optimized yet
        destination.mkdir(exist_ok=True)
        for f in self.iterdir():
            rel = str(self)
            if not rel.endswith("/"):
                rel = rel + "/"

            rel_dest = str(f)[len(rel) :]
            f.download_to(destination / rel_dest)

        return destination


    def download_to(self, destination: str | os.PathLike[Any]) -> Path:
        destination = Path(destination)

        meta = self.client._get_path_meta(self)
        return self._download_to_with_meta(meta, destination)


    def upload_from(
        self,
        source: str | os.PathLike[Any],
        force_overwrite_to_cloud: bool | None = None,
    ) -> Self:
        """Upload a file or directory to the cloud path."""
        source = Path(source)

        if source.is_dir():
            for p in source.iterdir():
                (self / p.name).upload_from(p, force_overwrite_to_cloud=force_overwrite_to_cloud)

            return self

        meta = self.client._get_path_meta(self)
        if meta.exists and meta.is_directory:
            dst = self / source.name
            dst_meta = self.client._get_path_meta(dst)
        else:
            dst = self
            dst_meta = meta

        dst._upload_file_to_cloud_with_meta(
            dst_meta, source, force_overwrite_to_cloud=force_overwrite_to_cloud
        )

        return dst

    @overload
    def copy(
        self,
        destination: Self,
        force_overwrite_to_cloud: bool | None = None,
    ) -> Self: ...

    @overload
    def copy(
        self,
        destination: Path,
        force_overwrite_to_cloud: bool | None = None,
    ) -> Path: ...

    @overload
    def copy(
        self,
        destination: str,
        force_overwrite_to_cloud: bool | None = None,
    ) -> Path | CloudPath: ...

    def copy(
        self,
        destination: str | os.PathLike[Any],
        force_overwrite_to_cloud: bool | None = None,
    ) -> Path | Any | CloudPath:
        """Copy self to destination folder of file, if self is a file."""
        meta = self.client._get_path_meta(self)
        if not meta.exists or meta.is_directory:
            raise ValueError(
                f"Path {self} should be a file. To copy a directory tree use the method copytree."
            )

        # handle string version of cloud paths + local paths
        if isinstance(destination, str | os.PathLike):
            destination = anypath.to_anypath(destination)

        if not isinstance(destination, CloudPath):
            return self._download_to_with_meta(meta, destination)

        # if same client, use cloud-native _move_file on client to avoid downloading
        if self.client is destination.client:
            destination_meta = destination.client._get_path_meta(destination)
            if destination_meta.exists and destination_meta.is_directory:
                destination = destination / self.name
                # Need to refresh metadata after updating destination
                destination_meta = destination.client._get_path_meta(destination)

            if force_overwrite_to_cloud is None:
                force_overwrite_to_cloud = os.environ.get(
                    "CLOUDPATHLIB_FORCE_OVERWRITE_TO_CLOUD", "False"
                ).lower() in ["1", "true"]

            if meta.last_modified is None:
                raise ValueError("Last modified time is not available for the source file")

            st_mtime = meta.last_modified.timestamp()

            if (
                not force_overwrite_to_cloud
                and destination_meta.exists
                and destination_meta.last_modified.timestamp() >= st_mtime
            ):
                raise OverwriteNewerCloudError(
                    f"File ({destination}) is newer than ({self}). "
                    f"To overwrite "
                    f"pass `force_overwrite_to_cloud=True`."
                )

            return self.client._move_file(self, destination, remove_src=False)  # type: ignore

        if not destination.exists() or destination.is_file():
            return destination.upload_from(
                self.fspath, force_overwrite_to_cloud=force_overwrite_to_cloud
            )

        return (destination / self.name).upload_from(
            self.fspath, force_overwrite_to_cloud=force_overwrite_to_cloud
        )

    # ===========  private cloud methods ===============

    def _refresh_cache_with_meta(
        self, meta: "AzurePathProperties", force_overwrite_from_cloud: bool | None = None
    ) -> None:
        if not meta.exists:
            # nothing to cache if the file does not exist; happens when creating
            # new files that will be uploaded
            return

        if force_overwrite_from_cloud is None:
            force_overwrite_from_cloud = os.environ.get(
                "CLOUDPATHLIB_FORCE_OVERWRITE_FROM_CLOUD", "False"
            ).lower() in [
                "1",
                "true",
            ]

        if meta.last_modified is None:
            raise ValueError("Last modified time is not available for the source file")

        st_mtime = meta.last_modified.timestamp()

        # if not exist or cloud newer
        if (
            force_overwrite_from_cloud
            or not self._local.exists()
            or (self._local.stat().st_mtime < st_mtime)
        ):
            # ensure there is a home for the file
            self._local.parent.mkdir(parents=True, exist_ok=True)
            self._download_to_with_meta(meta, self._local)

            # force cache time to match cloud times
            os.utime(self._local, times=(st_mtime, st_mtime))

        if self._dirty:
            raise OverwriteDirtyFileError(
                f"Local file ({self._local}) for cloud path ({self}) has been changed by your code, but "
                f"is being requested for download from cloud. Either (1) push your changes to the cloud, "
                f"(2) remove the local file, or (3) pass `force_overwrite_from_cloud=True` to "
                f"overwrite; or set env var CLOUDPATHLIB_FORCE_OVERWRITE_FROM_CLOUD=1."
            )

        # if local newer but not dirty, it was updated
        # by a separate process; do not overwrite unless forced to
        if self._local.stat().st_mtime > st_mtime:
            raise OverwriteNewerLocalError(
                f"Local file ({self._local}) for cloud path ({self}) is newer on disk, but "
                f"is being requested for download from cloud. Either (1) push your changes to the cloud, "
                f"(2) remove the local file, or (3) pass `force_overwrite_from_cloud=True` to "
                f"overwrite; or set env var CLOUDPATHLIB_FORCE_OVERWRITE_FROM_CLOUD=1."
            )

    def _refresh_cache(self, force_overwrite_from_cloud: bool | None = None) -> None:
        meta = self.client._get_path_meta(self)
        self._refresh_cache_with_meta(meta, force_overwrite_from_cloud=force_overwrite_from_cloud)

    def _upload_local_to_cloud_with_meta(
        self,
        meta: "AzurePathProperties",
        force_overwrite_to_cloud: bool | None = None,
    ) -> Self:
        """Uploads cache file at self._local to the cloud"""
        # We should never try to be syncing entire directories; we should only
        # cache and upload individual files.
        if self._local.is_dir():
            raise ValueError("Only individual files can be uploaded to the cloud")

        uploaded = self._upload_file_to_cloud_with_meta(
            meta=meta, local_path=self._local, force_overwrite_to_cloud=force_overwrite_to_cloud
        )

        # we need to refresh the metadata after the upload
        meta = self.client._get_path_meta(self)

        if meta.last_modified is None:
            raise ValueError("Last modified time is not available for the source file")

        # force cache time to match cloud times
        st_mtime = meta.last_modified.timestamp()
        os.utime(self._local, times=(st_mtime, st_mtime))

        # reset dirty and handle now that this is uploaded
        self._dirty = False
        self._handle = None

        return uploaded

    def _upload_local_to_cloud(self, force_overwrite_to_cloud: bool | None = None) -> Self:
        meta = self.client._get_path_meta(self)
        return self._upload_local_to_cloud_with_meta(
            meta, force_overwrite_to_cloud=force_overwrite_to_cloud
        )

    def _upload_file_to_cloud_with_meta(
        self,
        meta: "AzurePathProperties",
        local_path: Path,
        force_overwrite_to_cloud: bool | None = None,
    ) -> Self:
        """Uploads file at `local_path` to the cloud if there is not a newer file
        already there.
        """
        if force_overwrite_to_cloud is None:
            force_overwrite_to_cloud = os.environ.get(
                "CLOUDPATHLIB_FORCE_OVERWRITE_TO_CLOUD", "False"
            ).lower() in [
                "1",
                "true"
            ]

        if force_overwrite_to_cloud:
            # If we are overwriting no need to perform any checks, so we can save time
            self.client._upload_file(
                local_path,
                self,
            )
            return self

        st_mtime = meta.last_modified.timestamp() if meta.last_modified is not None else None

        # if cloud does not exist or local is newer, do the upload
        if not st_mtime or (local_path.stat().st_mtime > st_mtime):
            self.client._upload_file(
                local_path,
                self,
            )

            return self

        # cloud is newer and we are not overwriting
        raise OverwriteNewerCloudError(
            f"Local file ({self._local}) for cloud path ({self}) is newer in the cloud disk, but "
            f"is being requested to be uploaded to the cloud. Either (1) redownload changes from the cloud or "
            f"(2) pass `force_overwrite_to_cloud=True` to "
            f"overwrite; or set env var CLOUDPATHLIB_FORCE_OVERWRITE_TO_CLOUD=1."
        )

    def _upload_file_to_cloud(
        self,
        local_path: Path,
        force_overwrite_to_cloud: bool | None = None,
    ) -> Self:
        meta = self.client._get_path_meta(self)
        return self._upload_file_to_cloud_with_meta(
            meta, local_path, force_overwrite_to_cloud=force_overwrite_to_cloud
        )
