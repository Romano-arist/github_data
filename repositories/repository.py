import os
import random
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path
from time import sleep
from typing import Sequence, TypeVar
from datetime import datetime

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
from pydantic import BaseModel
from loguru import logger

from github import Github

from db import get_s3_client
from schemas import GitHubRepositorySchema, Interval, RepositorySortField, GitHubCommitSchema, GitHubCommitFileSchema, \
    GitHubCommitFileStates


T = TypeVar('T', bound=BaseModel)


class ApiRepository(ABC):
    """
    Abstract base class for API repositories.
    """

    def __init__(self,
                 *,
                 github_client: Github,
                 s3_client: boto3.client = None,
                 s3_file_system: s3fs.S3FileSystem = None,
                 s3_bucket_name: str,
                 s3_project_path: str,
                 s3_file_key: str,
                 ):
        self._github_client = github_client
        self._s3_client = s3_client or get_s3_client()
        self._s3_storage = S3StorageRepository(s3_client=self._s3_client, s3_file_system=s3_file_system)
        self._bucket_name = s3_bucket_name
        self._path = s3_project_path
        self._file_key = s3_file_key

    @abstractmethod
    def download_data(self, *args, **kwargs) -> Sequence[T]:
        """
        Download data from the API.
        """
        pass

    @abstractmethod
    def save_data(self, *args, **kwargs) -> bool:
        """
        Save data to the storage.
        """
        pass

    @abstractmethod
    def load_data(self, *args, **kwargs) -> pd.DataFrame:
        """
        Load data from the storage.
        """
        pass


class S3StorageRepository:
    def __init__(self, s3_client: boto3.client, s3_file_system: s3fs.S3FileSystem = None):
        self._s3_client = s3_client
        self._s3_file_system = s3_file_system or s3fs.S3FileSystem(
            anon=True,
            client_kwargs={'endpoint_url': self._s3_client.meta.endpoint_url},
            use_listings_cache=False
        )

    def save_data_as_parquet(self,
                             data: Sequence[T],
                             bucket_name: str,
                             path: str,
                             file_key: str,
                             partition_cols: Sequence[str] = None,
                             sort_columns: Sequence[str] = None,
                             exclude_columns: Sequence[str] = None,
                             ):
        """
        Save data to S3 storage in Parquet format with optional partitioning.
        """
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        data = [schema.model_dump() | {"insert_timestamp": timestamp} for schema in data]
        df = pd.DataFrame(data)
        if exclude_columns:
            assert all(col in df.columns for col in exclude_columns), "Some exclude columns are not in the DataFrame"
            df = df.drop(columns=list(exclude_columns), errors='ignore')
        if sort_columns:
            assert all(col in df.columns for col in sort_columns), "Some sort columns are not in the DataFrame"
            df.sort_values(by=sort_columns, inplace=True)
        if partition_cols:
            assert all(col in df.columns for col in partition_cols), "Some partition columns are not in the DataFrame"

        with tempfile.TemporaryDirectory() as temp_dir:
            if not partition_cols:
                table = pa.Table.from_pandas(df)
                file_path = os.path.join(temp_dir, file_key)
                pq.write_table(table, file_path)
                logger.info(f"Uploading {file_path} to S3 bucket {bucket_name} at path {path}")
                self._s3_client.upload_file(file_path, bucket_name, f"{path}/{file_key}.parquet")
            else:
                table = pa.Table.from_pandas(df)
                pq.write_to_dataset(
                    table,
                    root_path=temp_dir,
                    basename_template=f"{file_key}-" + "{i}.parquet",
                    partition_cols=partition_cols
                )

                for root, _, files in os.walk(temp_dir):
                    for file in files:
                        local_path = os.path.join(root, file)
                        relative_path = os.path.relpath(local_path, temp_dir)
                        s3_key = f"{path}/{relative_path}"
                        logger.info(f"Uploading {local_path} to S3 bucket {bucket_name} at path {s3_key}")
                        self._s3_client.upload_file(local_path, bucket_name, s3_key)

    def load_data_as_pandas(self, bucket_name: str, path: str, file_key: str) -> pd.DataFrame:
        """
        Load data from the storage (only for small files).
        """
        with self._s3_file_system.open(f'{bucket_name}/{path}/{file_key}.parquet', 'rb') as file:
            parquet_file = pq.ParquetFile(file)
            df = parquet_file.read().to_pandas()
            return df

    def load_all_parquet_files_from_path_as_pandas(self, bucket_name: str, path: str) -> pd.DataFrame:
        """
        Load and concatenate all Parquet files from the specified S3 path into a single DataFrame.
        Suitable for small data size. Should be replaced with a more efficient solution for larger datasets.
        """
        prefix = f"{bucket_name}/{path}"

        try:
            files = self._s3_file_system.ls(prefix)
        except FileNotFoundError:
            logger.warning(f"Path {prefix} not found in S3 bucket {bucket_name}")
            return pd.DataFrame()
        parquet_files = [f for f in files if f.endswith(".parquet")]

        dataframes = []
        for file_path in parquet_files:
            with self._s3_file_system.open(file_path, 'rb') as file:
                parquet_file = pq.ParquetFile(file)
                df = parquet_file.read().to_pandas()
                df["file_path"] = file_path
                dataframes.append(df)

        if dataframes:
            return pd.concat(dataframes, ignore_index=True)
        else:
            return pd.DataFrame()

    def load_data_metadata(self, bucket_name: str, path: str, file_key: str):
        """
        Load metadata from the storage (only for small files).
        """
        with self._s3_file_system.open(f'{bucket_name}/{path}/{file_key}.parquet', 'rb') as file:
            parquet_file = pq.ParquetFile(file)
            return parquet_file.metadata


class GitHubRepository(ApiRepository):
    def __init__(self,
                 *,
                 github_client: Github,
                 s3_bucket_name: str = "github",
                 s3_project_path: str = "repositories",
                 s3_file_key: str = "repositories_0",
                 s3_client: boto3.client = None,
                 s3_file_system: s3fs.S3FileSystem = None,
                 repositories_per_file_threshold: int = 1000,
                 ):
        super().__init__(
            github_client=github_client,
            s3_client=s3_client,
            s3_file_system=s3_file_system,
            s3_bucket_name=s3_bucket_name,
            s3_project_path=s3_project_path,
            s3_file_key=s3_file_key
        )
        self._repositories_per_file_threshold = repositories_per_file_threshold

    def download_data(self,
                      interval: Interval,
                      order_column: RepositorySortField = RepositorySortField.STARS,
                      stars: int = 0,
                      forks: int = 0
                      ) -> Sequence[GitHubRepositorySchema]:
        """
        Download repositories from GitHub based on the creation date (between from_timestamp and to_timestamp),
        ordered by stars desc.
        """
        query = (f"created:{interval.start_ts.strftime('%Y-%m-%dT%H:%M:%S')}.."
                 f"{interval.end_ts.strftime('%Y-%m-%dT%H:%M:%S')}"
                 # f"stars:>{stars} forks:>{forks}"
                 )
        if stars > 0:
            query += f" stars:>{stars}"
        if forks > 0:
            query += f" forks:>{forks}"
        logger.info(f"Downloading repositories from GitHub with query: {query}")
        repos = self._github_client.search_repositories(
            query=query,
            sort=order_column.value,
            order="desc"
        )
        logger.success(f"Downloaded {repos.totalCount} repositories from GitHub")
        return [GitHubRepositorySchema(id=rep.id, created_at=rep.created_at) for rep in repos]

    def save_data(self,
                  data: Sequence[GitHubRepositorySchema],
                  partition_cols: Sequence[str] | None = None,
                  is_new_data: bool = True,
                  ) -> bool:
        """
        Save repositories to S3 storage in Parquet format with optional partitioning.
        If the file already exists, it will be loaded and merged with the new data.
        If the new data exceed the threshold, it will not be saved.
        """
        if not data:
            logger.warning("No data to save")
            return False
        if is_new_data:
            try:
                projects_num = self._s3_storage.load_data_metadata(bucket_name=self._bucket_name,
                                                                   path=self._path,
                                                                   file_key=self._file_key).num_rows
                logger.info(f"File {self._file_key} has {projects_num} rows")
                if projects_num >= self._repositories_per_file_threshold:
                    logger.warning(f"File {self._file_key} has too many rows per one github token: {projects_num}")
                    return False
                elif projects_num + len(data) > self._repositories_per_file_threshold:
                    allowed = self._repositories_per_file_threshold - projects_num
                    logger.warning(f"File {self._file_key} has too many rows per one github token, "
                                   f"saving only {allowed} rows")
                    data = data[:allowed]
            except FileNotFoundError:
                logger.info(f"File {self._file_key} not found. Creating a new one.")

            # Get existing repositories from S3 bucket
            df = self._s3_storage.load_all_parquet_files_from_path_as_pandas(bucket_name=self._bucket_name,
                                                                             path=self._path)
            # Remove repositories that already exist in S3 bucket
            repositories = self._concat_repositories(old_repos=df, repos=data)
            logger.info(f"Got {len(repositories) - len(df)} new repositories")
        else:
            repositories = set(data)

        logger.info(f"Saving {len(repositories)} repositories to S3 bucket {self._bucket_name} at path {self._path}")
        self._s3_storage.save_data_as_parquet(
            data=repositories,
            bucket_name=self._bucket_name,
            path=self._path,
            file_key=f"{self._file_key}",
            partition_cols=partition_cols,
            sort_columns=["id"],
        )
        return True

    def load_data(self) -> pd.DataFrame:
        """
        Load data from the storage (only for small files).
        """
        df = self._s3_storage.load_data_as_pandas(
            bucket_name=self._bucket_name,
            path=self._path,
            file_key=self._file_key
        )
        return df

    def _concat_repositories(self,
                             *,
                             old_repos: pd.DataFrame,
                             repos: Sequence[GitHubRepositorySchema]) -> set[GitHubRepositorySchema]:
        """
        Concatenate new repositories with existing ones and remove duplicates.
        """
        if not repos:
            logger.warning("No new repositories to save")
            return old_repos
        repos = set(repos)

        repositories_set = {GitHubRepositorySchema(**repo) for repo in old_repos.to_dict(orient='records')}
        for repo in repos:
            if repo not in repositories_set:
                repositories_set.add(repo)
        return repositories_set


class GitHubCommits(ApiRepository):
    def __init__(self,
                 *,
                 github_client: Github,
                 s3_client: boto3.client = None,
                 s3_file_system: s3fs.S3FileSystem = None,
                 s3_bucket_name: str = "github",
                 s3_project_path: str = "commits",
                 s3_file_key: str = "commits",
                 ):
        super().__init__(
            github_client=github_client,
            s3_client=s3_client,
            s3_file_system=s3_file_system,
            s3_bucket_name=s3_bucket_name,
            s3_project_path=s3_project_path,
            s3_file_key=s3_file_key
        )

    def download_data(self,
                      repository_id: int,
                      since_datetime: datetime | None = None,
                      until_datetime: datetime | None = None) -> Sequence[GitHubCommitSchema]:
        """
        Download commits from GitHub for a given repository.
        """
        logger.info(f"Downloading commits for repository {repository_id}")
        repository_from_api = self._github_client.get_repo(repository_id)
        commit_date_intervals_kwargs = {}
        if since_datetime:
            commit_date_intervals_kwargs["since"] = since_datetime
        if until_datetime:
            commit_date_intervals_kwargs["until"] = until_datetime
        logger.info(f"Downloading commits for repository {repository_id} "
                    f"with params: {commit_date_intervals_kwargs if commit_date_intervals_kwargs else 'all commits'}")

        commits = []
        for commit in repository_from_api.get_commits(**commit_date_intervals_kwargs):
            logger.info(f"Downloading commit {commit.sha} for repository {repository_id}")
            files = []
            files_states = []
            for file in commit.files:
                if file.status != "removed":
                    try:
                        file_blob = repository_from_api.get_git_blob(file.sha)
                    except Exception as e:
                        logger.error(f"Error downloading file blob {file.sha} for commit {commit.sha}: {e}")
                        file_blob = None
                    files_states.append(
                        GitHubCommitFileStates(
                            repository_id=repository_id,
                            commit_sha=commit.sha,
                            commit_at=commit.commit.committer.date,
                            file_sha=file.sha,
                            file_size=file_blob.size if file_blob else None,
                            raw_url=file.raw_url
                        )
                    )
                # save file from commit
                files.append(
                    GitHubCommitFileSchema(
                        repository_id=repository_id,
                        commit_sha=commit.sha,
                        commit_at=commit.commit.committer.date,
                        file_sha=file.sha,
                        file_path=file.filename,
                        file_type=Path(file.filename).suffix,
                        status=file.status,
                        lines_added=file.additions,
                        lines_deleted=file.deletions,
                        changes=file.changes
                    )
                )

            logger.success(f"Downloaded {len(files)} files  "
                        f"for commit {commit.sha} in repository {repository_id}")
            ghcs = GitHubCommitSchema(
                repository_id=repository_id,
                commit_sha=commit.sha,
                commit_author_id=commit.author.id if commit.author else None,
                commit_at=commit.commit.committer.date,
                commit_message=commit.commit.message,
                num_files_changed=len(files),
                num_lines_added=sum(file.lines_added for file in files),
                num_lines_deleted=sum(file.lines_deleted for file in files),
                parent_commits=[parent.sha for parent in commit.parents],
                files=files,
                file_states=files_states,
            )
            logger.success(f"Downloaded commit {ghcs.commit_sha} for repository {repository_id}")
            commits.append(ghcs)
            sleep(random.uniform(0.5, 1.5)) # check if it helps for github limits
        logger.success(f"Downloaded {len(commits)} commits for repository {repository_id}")
        return commits

    def save_data(self, repository_id: int, schemas: Sequence[T], exclude_columns=("files", "file_states", )) -> bool:
        """
        Save commits to S3 storage in Parquet format with partitioning.
        To prevent duplicates need to load existing data from S3 bucket (by partitions) and merge with new data.
        """
        logger.info(f"Saving data for repository {repository_id} to S3 bucket {self._bucket_name} at path {self._path}")
        # assume that file_name is constant for all commits by repository and commit_month
        self._s3_storage.save_data_as_parquet(
            data=schemas,
            bucket_name=self._bucket_name,
            path=self._path,
            file_key=f"{self._file_key}_{repository_id}",
            partition_cols=["repository_id", "commit_month"],
            exclude_columns=exclude_columns
        )
        return True

    def load_data(self, *args, **kwargs) -> pd.DataFrame:
        raise NotImplementedError("Load data method is not implemented for GitHubCommits class")


class GitHubFileChanges(GitHubCommits):
    def __init__(self,
                 *,
                 github_client: Github,
                 s3_client: boto3.client = None,
                 s3_file_system: s3fs.S3FileSystem = None,
                 s3_bucket_name: str = "github",
                 s3_project_path: str = "file_changes",
                 s3_file_key: str = "file_changes",
                 ):
        super().__init__(
            github_client=github_client,
            s3_client=s3_client,
            s3_file_system=s3_file_system,
            s3_bucket_name=s3_bucket_name,
            s3_project_path=s3_project_path,
            s3_file_key=s3_file_key
        )
        self._s3_file_system = s3_file_system

    def download_data(self,
                      repository_id: int,
                      since_datetime: datetime | None = None,
                      until_datetime: datetime | None = None) -> Sequence[GitHubCommitFileSchema]:
        """
        Download file changes from GitHub for a given repository.
        Uses GitHubCommits class to get commits and their files.
        """
        logger.info(f"Downloading file changes for repository {repository_id}")
        commits = super().download_data(repository_id=repository_id,
                                        since_datetime=since_datetime,
                                        until_datetime=until_datetime)
        files_changes = [commit.files for commit in commits]
        logger.success(f"Downloaded {len(files_changes)} file changes for repository {repository_id}")
        return files_changes

    def save_data(self, repository_id: int, schemas: Sequence[T], exclude_columns = None) -> bool:
        super().save_data(
            repository_id=repository_id,
            schemas=schemas,
            exclude_columns=exclude_columns,
        )

    def load_data(self):
        raise NotImplementedError("Load data method is not implemented for GitHubFileChanges class")


class GitHubFileStates(GitHubCommits):
    def __init__(self,
                 *,
                 github_client: Github,
                 s3_client: boto3.client = None,
                 s3_file_system: s3fs.S3FileSystem = None,
                 s3_bucket_name: str = "github",
                 s3_project_path: str = "file_states",
                 s3_file_key: str = "file_states",
                 ):
        super().__init__(
            github_client=github_client,
            s3_client=s3_client,
            s3_file_system=s3_file_system,
            s3_bucket_name=s3_bucket_name,
            s3_project_path=s3_project_path,
            s3_file_key=s3_file_key
        )
        self._s3_file_system = s3_file_system

    def download_data(self,
                      repository_id: int,
                      since_datetime: datetime | None = None,
                      until_datetime: datetime | None = None) -> Sequence[GitHubCommitFileStates]:
        """
        Download file states from GitHub for a given repository.
        Uses GitHubCommits class to get commits and their files.
        """
        logger.info(f"Downloading file states for repository {repository_id}")
        commits = super().download_data(repository_id=repository_id,
                                        since_datetime=since_datetime,
                                        until_datetime=until_datetime)
        files_states = [commit.file_states for commit in commits]
        logger.success(f"Downloaded {len(files_states)} file sates for repository {repository_id}")
        return files_states

    def save_data(self, repository_id: int, schemas: Sequence[T], exclude_columns = None) -> bool:
        super().save_data(
            repository_id=repository_id,
            schemas=schemas,
            exclude_columns=exclude_columns,
        )

    def load_data(self):
        raise NotImplementedError("Load data method is not implemented for GitHubFileStates class")





