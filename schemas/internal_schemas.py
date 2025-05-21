from dataclasses import dataclass
from datetime import datetime
from enum import Enum


@dataclass
class Interval:
    start_ts: datetime
    end_ts: datetime


class RepositorySortField(Enum):
    STARS = "stars"
    FORKS = "forks"
    UPDATED = "updated"

@dataclass
class TokensToGitHubRepository:
    token: str
    repository_parquet_name: str
