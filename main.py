import os
import pathlib
from collections.abc import Sequence
from datetime import datetime, timedelta

from loguru import logger

from db import get_s3_client
from schemas import TokensToGitHubRepository
from services import GitHubUploadToS3Service


def add_logger() -> None:
    logg_path = os.path.join(os.path.dirname(os.path.realpath(__file__)))
    pathlib.Path(os.path.dirname(logg_path)).mkdir(parents=True, exist_ok=True)
    logger.add(logg_path, format='{time} {level} {message}',
               level='DEBUG', rotation='1 week', compression='zip',
               serialize=True)

def get_github_api_tokens_models(github_api_tokens: dict[str, str]) -> Sequence[TokensToGitHubRepository]:
    return [
        TokensToGitHubRepository(token=token, repository_parquet_name=repository_parquet_name)
        for token, repository_parquet_name in github_api_tokens.items()
    ]

def init_repositories_upload_by_stars(
        github_api_tokens: dict[str, str],
        snapshot_datetime: datetime,
        start_ts: datetime,
        stop_ts: datetime,
        ts_delta: timedelta,
        stars_count: int = 100,
        repositories_per_file_threshold: int = 1000,
        s3_client=get_s3_client(),
):
    """
    Initialize the upload of GitHub repositories to S3 by stars count.
    """
    logger.add("logs/logs.log", format='{time} {level} {message}',
               level='INFO', rotation='1 week', compression='zip',
               serialize=True)
    github_service = GitHubUploadToS3Service(
        github_api_tokens=get_github_api_tokens_models(github_api_tokens),
        snapshot_datetime=snapshot_datetime,  # redundant
        s3_client=s3_client
    )
    github_service.init_repositories_upload_by_stars(
    start_ts=start_ts,
    stop_ts=stop_ts,
    ts_delta=ts_delta,
    stars_count=stars_count,
    repositories_per_file_threshold=repositories_per_file_threshold
    )


def main(
        github_api_tokens: dict[str, str],
        snapshot_datetime: datetime,
        s3_client=get_s3_client(),
        workers_count: int = 10,
):
    """
    Main function to upload increment for GitHub repositories to S3.
    """
    logger.add("logs/logs.log", format='{time} {level} {message}',
               level='INFO', rotation='1 week', compression='zip',
               serialize=True)
    github_service = GitHubUploadToS3Service(
        github_api_tokens=get_github_api_tokens_models(github_api_tokens),
        snapshot_datetime=snapshot_datetime,
        s3_client=s3_client
    )
    github_service.retrieve_and_save(workers=workers_count)


if __name__ == '__main__':
    tokens = {
        "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": "repositories_0",
        "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": "repositories_0",
        "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": "repositories_0",
    }
    snapshot_ts = datetime(2024, 11, 1)
    s3 = get_s3_client()

    # init_repositories_upload_by_stars(
    #     github_api_tokens=tokens,
    #     snapshot_datetime=snapshot_ts,
    #     s3_client=s3,
    #     start_ts=datetime(2023, 11, 1),
    #     stop_ts=datetime(2025, 5, 20),
    #     ts_delta=timedelta(days=3),
    #     stars_count=100,
    #     repositories_per_file_threshold=1000
    # )

    main(
        github_api_tokens=tokens,
        snapshot_datetime=snapshot_ts,
        s3_client=s3,
        workers_count=10
    )
