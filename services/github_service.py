from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from datetime import datetime, timezone, timedelta
from collections.abc import Sequence

import requests.exceptions
from urllib3.util import Retry
from github import Github, RateLimitExceededException
import boto3
import pandas as pd
from loguru import logger

from repositories import GitHubCommits, GitHubRepository, GitHubFileChanges, GitHubFileStates
from schemas import TokensToGitHubRepository, GitHubRepositorySchema, RepositorySortField
from utils import get_intervals


class GitHubUploadToS3Service:
    def __init__(self, github_api_tokens: list[TokensToGitHubRepository], snapshot_datetime: datetime,
                 s3_client: boto3.client = None):
        self.__github_api_tokens = github_api_tokens
        self.snapshot_datetime = snapshot_datetime.replace(tzinfo=timezone.utc)
        self.s3_client = s3_client
        self.retry = Retry(
            total=0,
            status_forcelist=[403],
            raise_on_status=True
        )
        self.__service_lock = Lock()
        self.__current_token = None
        self._attempts = len(github_api_tokens) * 4

    def init_repositories_upload_by_stars(self, start_ts: datetime, stop_ts: datetime,
                                          ts_delta: timedelta, stars_count: int = 100,
                                          repositories_per_file_threshold: int = 1000):
        self.__current_token = self.__github_api_tokens.pop(0)
        try:
            github = Github(self.__current_token.token)
            interval = get_intervals(start_ts, stop_ts, ts_delta)
            github_repo = GitHubRepository(github_client=github,
                                           s3_client=self.s3_client,
                                           s3_file_key=self.__current_token.repository_parquet_name,
                                           repositories_per_file_threshold=repositories_per_file_threshold)
            # Loading repositories according to the interval
            for i in interval:
                res = github_repo.download_data(interval=i,
                                                order_column=RepositorySortField.STARS,
                                                stars=stars_count
                                                )
                if not res:
                    continue
                saved = github_repo.save_data(res, )
                if not saved:
                    break
        finally:
            self.__github_api_tokens.append(self.__current_token)

    def retrieve_and_save(self, workers=5):
        """
        Get data from GitHub and save it to S3.
        """
        self.__current_token = self.__github_api_tokens.pop(0)
        if not self.__current_token:
            raise ValueError("No tokens available.")

        github = Github(self.__current_token.token, retry=self.retry)
        github_repo = GitHubRepository(github_client=github, s3_client=self.s3_client,
                                       s3_file_key=self.__current_token.repository_parquet_name)
        # Loading existing data with repositories
        github_repo_df = github_repo.load_data()
        github_repo_df["last_commit_at"] = github_repo_df["last_commit_at"].fillna(
            pd.Timestamp("1970-01-01", tz="UTC")
        )
        # Getting repositories with last commit after snapshot datetime or without commit datetime
        filtered_github_repo_df = github_repo_df[
            (github_repo_df["last_commit_at"] >= self.snapshot_datetime) | (
                github_repo_df["last_commit_sha"].isna())
            ]
        filtered_github_repo_df = filtered_github_repo_df.sort_values(by="last_commit_at")
        # Distinguish filtered repositories from the rest
        not_filtered_df = [GitHubRepositorySchema(**row)
                           for row in pd.concat([github_repo_df,
                                                 filtered_github_repo_df]
                                                ).drop_duplicates(keep=False).to_dict(orient="records")]

        repos = [GitHubRepositorySchema(**row) for row in filtered_github_repo_df.to_dict(orient="records")]
        try:
            logger.info(f"Fetching increment data for {len(repos)} repositories, "
                        f"snapshot datetime: {self.snapshot_datetime}")
            self._submit_fetching_increment(github, repos, workers)
        finally:
            try:
                logger.info(f"Saving updated repositories to S3 bucket github at path repositories")
                github_repo.save_data(
                    data=repos + not_filtered_df,
                    is_new_data=False,
                )
                self.__github_api_tokens.append(self.__current_token)
            except Exception as ex:
                logger.error(f"Error saving repositories data to S3: {ex}")

    def _submit_fetching_increment(self, github_client: Github,
                                   repositories: Sequence[GitHubRepositorySchema],
                                   max_workers: int):
        """
        Submit the fetching of increment data for each repository to a thread pool executor.
        Add cancellation and error handling.
        """
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for repo in repositories:
                executor.submit(self._fetch_increment_worker, repo, github_client)

    def _fetch_increment_worker(self, repository: GitHubRepositorySchema, github_client: Github):
        """
        Fetch increment data for a single repository and save it to S3.
        """
        while True:
            try:
                # Loading and save commits
                github_commits = GitHubCommits(github_client=github_client, s3_client=self.s3_client)
                # Download new data from the start of last commit month - to avoid rewriting partitioned data
                start_of_month = repository.last_commit_at.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                github_commits_schemas = github_commits.download_data(repository_id=repository.id,
                                                                      since_datetime=start_of_month)
                if not github_commits_schemas:
                    return
                github_commits.save_data(repository_id=repository.id, schemas=github_commits_schemas)

                # Save file changes
                files_changes = [commit.files for commit in github_commits_schemas]
                file_changes_schemas = [schema for schema_lst in files_changes for schema in schema_lst]
                GitHubFileChanges(github_client=github_client, s3_client=self.s3_client).save_data(
                    repository_id=repository.id,
                    schemas=file_changes_schemas,
                )

                # Save file states
                file_states = [commit.file_states for commit in github_commits_schemas]
                file_states_schemas = [schema for schema_lst in file_states for schema in schema_lst]
                GitHubFileStates(github_client=github_client, s3_client=self.s3_client).save_data(
                    repository_id=repository.id,
                    schemas=file_states_schemas, )

                # Update repository data
                repository.last_commit_at = github_commits_schemas[0].commit_at
                repository.last_commit_sha = github_commits_schemas[0].commit_sha
                return

            except (RateLimitExceededException, requests.exceptions.RetryError) as rex:
                if isinstance(rex, RateLimitExceededException):
                    logger.warning(f"Rate limit exceeded for GitHub API: {rex}")
                else:
                    logger.warning(f"Retry error: {rex}")
                with self.__service_lock:
                    self._attempts -= 1
                    if self._attempts <= 0:
                        logger.error("Max attempts reached. Exiting.")
                        raise rex
                    logger.info("Rate limit exceeded for GitHub API. Trying to switch to another token.")
                    limit = github_client.get_rate_limit().core.remaining
                    if limit <= 0:
                        if len(self.__github_api_tokens) == 0:
                            logger.error("No more tokens available.")
                            raise
                        self.__github_api_tokens.append(self.__current_token)
                        self.__current_token = self.__github_api_tokens.pop(0)
                        github_client = Github(self.__current_token.token, retry=self.retry)
                        logger.info(f"Switched to token {self.__current_token.token}")
            except Exception as ex:
                logger.error(f"Error fetching data for repository {repository.id}: {ex}")
                raise ex

