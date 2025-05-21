from collections.abc import Sequence
from datetime import datetime, date
from email.policy import default

from pydantic import BaseModel, Field, computed_field


class GitHubRepositorySchema(BaseModel):
    id: int = Field(gt=0, description="Repository ID")
    created_at: datetime = Field(description="Repository creation datetime")
    last_commit_sha: str | None = Field(default=None, description="Last commit SHA")
    last_commit_at: datetime | None = Field(default=None, description="Last commit datetime")

    @computed_field
    def created_date(self) -> date:
        """Calculate the date of created_at timestamp."""
        return self.created_at.date()

    def __eq__(self, other):
        if isinstance(other, GitHubRepositorySchema):
            return self.id == other.id
        return False

    def __hash__(self):
        return self.id


class GitHubCommitFileSchema(BaseModel):
    repository_id: int = Field(description="Repository ID")
    commit_sha: str = Field(min_length=40, max_length=40, description="Commit SHA")
    file_sha: str = Field(min_length=40, max_length=40, description="File SHA")
    commit_at: datetime = Field(description="Commit datetime")
    file_path: str = Field(min_length=1, description="File path")
    file_type: str = Field(description="File type")
    status: str = Field(description="File status")
    lines_added: int = Field(ge=0, description="Number of lines added")
    lines_deleted: int = Field(ge=0, description="Number of lines deleted")
    changes: int = Field(ge=0, description="Number of changes")

    @computed_field
    def commit_month(self) -> str:
        """Calculate the month and the year of commit_at timestamp."""
        return self.commit_at.date().strftime("%Y-%m")

    def __eq__(self, other):
        if isinstance(other, GitHubCommitFileSchema):
            return (self.commit_sha == other.commit_sha and self.file_path == other.file_path
                    and self.repository_id == other.repository_id)
        return False

    def __hash__(self):
        return hash((self.commit_sha, self.file_sha, self.repository_id))


class GitHubCommitFileStates(BaseModel):
    repository_id: int = Field(description="Repository ID")
    commit_sha: str = Field(min_length=40, max_length=40, description="Commit SHA")
    commit_at: datetime = Field(description="Commit datetime")
    file_sha: str = Field(min_length=40, max_length=40, description="File SHA")
    file_size: int | None = Field(description="File size in bytes")
    s3_file_location: str = Field(default="", description="S3 file location, if exists") # temporary allow empty string
    raw_url: str = Field(description="Raw URL, if exists")

    @computed_field
    def commit_month(self) -> str:
        """Calculate the month and the year of commit_at timestamp."""
        return self.commit_at.date().strftime("%Y-%m")


class GitHubCommitSchema(BaseModel):
    repository_id: int = Field(description="Repository ID")
    commit_sha: str = Field(min_length=40, max_length=40, description="Commit SHA")
    commit_author_id: int = Field(description="Commit author ID")
    commit_at: datetime = Field(description="Commit datetime")
    commit_message: str = Field(min_length=1, description="Commit message")
    num_files_changed: int = Field(ge=0, description="Number of files changed")
    num_lines_added: int = Field(ge=0, description="Number of lines added")
    num_lines_deleted: int = Field(ge=0, description="Number of lines deleted")
    parent_commits: Sequence[str] = Field(description="List of parent commit SHAs")
    files: Sequence[GitHubCommitFileSchema] = Field(description="List of changed files")
    file_states: Sequence[GitHubCommitFileStates] = Field(description="List of file states")

    @computed_field
    def commit_month(self) -> str:
        """Calculate the month and the year of commit_at timestamp."""
        return self.commit_at.date().strftime("%Y-%m")

    def __eq__(self, other):
        if isinstance(other, GitHubCommitSchema):
            return self.commit_sha == other.commit_sha and self.repository_id == other.repository_id
        return False

    def __hash__(self):
        return hash((self.commit_sha, self.repository_id))
