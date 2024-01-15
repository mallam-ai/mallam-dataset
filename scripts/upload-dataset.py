import json
from typing import List

import click
from datasets import load_dataset

from mallam_datasets.backend import BackendService
from mallam_datasets.state import State


@click.command()
@click.option("--repo_id", "repo_id", required=True, type=str)
@click.option("--team_id", "team_id", required=True, type=str)
@click.option("--user_id", "user_id", required=True, type=str)
@click.option("--limit", "limit", required=False, type=int, default=10)
def main(repo_id: str, team_id: str, user_id: str, limit: int):
    backend = BackendService()

    dataset = load_dataset(repo_id, split="train")

    with State("upload-dataset.state.json") as s:
        if repo_id in s.data:
            start = s.data[repo_id]
        else:
            start = 0

        for i, row in enumerate(dataset):
            if i < start:
                continue

            print(f"{i + 1}/{len(dataset)}")

            backend.invoke(
                "document_create",
                title=row["title"],
                content=row["content"],
                teamId=team_id,
                userId=user_id,
            )

            s.data[repo_id] = i + 1

            if i - start >= limit:
                break


if __name__ == "__main__":
    main()
