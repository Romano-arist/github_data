# GitHub Data to S3
This project is designed to incrementally export commit data from GitHub repositories to an S3-compatible storage in a Parquet format. 
The core idea is to treat repositories as the central unit for data extraction.

## Agenda
- **Future queries**: The system is designed to support future queries on commit data for ML purposes. I assumed that queries will be
  performed on the commit data, not on the repository metadata.
- **Repositories as the core entity**: The system is designed around GitHub repositories as the main tracked entities, enabling regular monitoring and commit extraction.
- **Data schema**: The schema is organized into four logical tables:
  - repositories
  - commits
  - file_changes
  - file_states
- **Partitioning strategy**: All tables except repositories are partitioned by repository_id and commit_month. This provides:
  - Efficient reconstruction of repository state at a specific point in time.
  - The ability to link related commits. 
  - De-duplication in case of reprocessing (relevant in this case when writing-only to S3, without reading during ingestion).
- **Immutable fields**: Each table contains immutable columns to avoid maintaining historical versions of records at this stage of the development.
- **Simplified writing**: To avoid reading existing data during writes (and to reduce dependencies such as Spark), 
the commit-related tables are written in partitions by repositories_id and commit_month={YYYY-MM}. This monthly granularity is currently sufficient, and files are overwritten in case of recomputation.
- **File content is not stored**: At this stage, the actual contents of files are not saved in S3. Only links to the file versions are preserved.

### Possible Improvements
- **Metadata decoupling**: Separate metadata about the latest processed commits from the repository records to reduce redundancy and avoid tight bounds. Consider storing such metadata in a relational database. 
- **De-duplication at write-time**: Introduce processing engines like Spark to prevent duplicates when writing new data.


> **Design note**:  
> The current partitioning strategy assumes that most queries will be scoped by specific repositories - for example,
> to trace commit history or reconstruct repository state over time.  
> Therefore, `repository_id` is used as the primary partition key.  
>  
> If most queries are expected to be time-based (e.g., filtering commits by date across many repositories),  
> it would make sense to revise the schema and switch to partitioning by commit timestamp instead.

# Data schema
### Repositories
- **Table location** : `s3://github/repositories/repositories_{i}.parquet`
- **Columns**:
  - `id`: int, unique identifier for the repository
  - `created_at`: datetime, repository creation datetime
  - `last_commit_sha`: string, SHA of the last processed commit
  - `last_commit_at`: datetime, datetime of the last processed commit

### Commits
- **Table location** : `s3://github/commits/repository_id={}/commit_month={YYYY-MM}/commits_{repository_id}-{i}.parquet`
- **Columns**:
  - `repository_id`: int, unique identifier for the repository
  - `commit_sha`: string, commit SHA
  - `commit_author_id`: int, unique identifier for the commit author
  - `commit_at`: datetime, commit datetime
  - `commit_message`: string, commit message
  - `num_files_changed`: int, number of files changed in the commit
  - `num_lines_added`: int, number of lines added in the commit
  - `num_lines_deleted`: int, number of lines deleted in the commit
  - `parent_commits`: string, list of parent commit SHAs
  - `commit_month`: string, month of the commit in the format YYYY-MM, the partition column

### File_changes
- **Table location** : `s3://github/file_changes/repository_id={}/commit_month={YYYY-MM}/file_changes_{repository_id}-{i}.parquet`
- **Columns**:
  - `repository_id`: int, unique identifier for the repository
  - `commit_sha`: string, commit SHA
  - `file_sha`: string, SHA of the file in the commit
  - `commit_at`: datetime, commit datetime
  - `file_path`: string, path of the file in the repository
  - `file_type`: string, file type (e.g., ".py", ".txt")
  - `status`: string, status of the file in the commit (e.g., "added", "modified", "removed")
  - `lines_added`: int, number of lines added in the file
  - `lines_deleted`: int, number of lines deleted in the file
  - `changes`: int, number of lines changed in the file
  - `commit_month`: string, month of the commit in the format YYYY-MM, the partition column

### File_states
  - **Table location** : `s3://github/file_states/repository_id={}/commit_month={YYYY-MM}/file_states_{repository_id}-{i}.parquet`
  - **Columns**:
    - `repository_id`: int, unique identifier for the repository
    - `commit_sha`: string, commit SHA
    - `file_sha`: string, SHA of the file in the commit
    - `commit_at`: datetime, commit datetime
    - `file_size`: int, size of the file in bytes
    - `s3_file_location`: string, S3 location of the file (currently not used, only raw_url instead)
    - `raw_url`: string, raw URL of the file in the repository
    - `commit_month`: string, month of the commit in the format YYYY-MM, the partition column

## Current Program Logic
1. **Input tokens and repository file**  
   The program takes a list of GitHub API tokens and a path to a file containing repositories to monitor.  
   If the repository file is missing, the user is prompted to download it.
2. **S3 client and snapshot date**  
   An S3-client and a snapshot date are provided as inputs.  
   The snapshot date is used as a point for selecting relevant commits.
3. **Filtering repositories**  
   The program iterates over the list of repositories and selects those whose latest recorded commit date  
   is earlier than the snapshot date, or is missing.
4. **Parallel commit loading with token rotation**  
   Commit and file change data are loaded in parallel using multiple threads.  
   If a token exceeds its API rate limit, the system automatically switches to the next available token.
5. **Update repositories metadata after uploading**  
   After writing data to S3, the program updates the `last_commit_at` and `last_commit_sha` for each repository  
   to avoid reprocessing the same data in future runs.

### Possible Improvements
- **Environment variables**: Use environment variables for configuration instead of hardcoding values in the code.
- **Update repositories metadata**: Update repositories metadata on each successful commit load, not just at the end of the process.
- **Data validation**: Implement data validation checks to ensure the integrity and consistency of the data being written to S3.
- **Testing**: Implement unit tests to ensure the correctness of the code and the reliability of the data loading process.

### Program launch
To run the program, use the following command:
1. Launch and initialize S3-Minio docker container: 
```bash
docker-compose up -d
```
2. Launch main.py, provide Github api tokens and path to the file with repositories:
```python
tokens = {
    "token_1": "repositories_0",
    "token_2": "repositories_0",
    "token_3": "repositories_0",
}
snapshot_ts = datetime(2024, 11, 1)
s3 = get_s3_client() # function to get a local S3 client

# Initialize repositories upload by stars
init_repositories_upload_by_stars(
    github_api_tokens=tokens,
    snapshot_datetime=snapshot_ts,
    s3_client=s3,
    start_ts=datetime(2023, 11, 1),
    stop_ts=datetime(2025, 5, 20),
    ts_delta=timedelta(days=3),
    stars_count=100,
    repositories_per_file_threshold=1000 # max number of repositories per file
)

# Load incremental data
main(
    github_api_tokens=tokens,
    snapshot_datetime=snapshot_ts,
    s3_client=s3,
    workers_count=10
)
```

### Dag
The program can be run as a DAG using Airflow. The DAG is defined in the `dags` directory and can be scheduled to run at regular intervals.

### Data snapshot
The current data snapshot is stored in the `minio-data` directory and can be accessed using the docker compose file (via S3Minio, as a bind mount).
