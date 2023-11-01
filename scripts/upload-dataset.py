import json
from typing import List

import click
import stanza
from datasets import load_dataset

from mallam_datasets.backend import BackendService
from mallam_datasets.state import State


@click.command()
@click.argument('repo_id', required=True, type=str)
def main(repo_id: str):
    backend = BackendService()

    pipeline = stanza.Pipeline(lang='en', processors='tokenize')

    def extract_sentences(text: str) -> List[str]:
        doc: stanza.Document = pipeline(text)
        return list(filter(None, map(lambda item: item.text.strip() if item else '', doc.sentences)))

    def upload_document(document_id: str, repo: str, url: str, title: str, content: str):
        sentences = extract_sentences(content)
        res = backend.invoke(
            'datasets_document_upsert',
            id=document_id,
            repo=repo,
            url=url,
            title=title,
            content=content,
            sentences=sentences,
        )
        if 'success' not in res:
            raise Exception(json.dumps(res))

    dataset = load_dataset(repo_id, split='train')

    with State('upload-dataset.state.json') as s:
        if repo_id in s.data:
            start = s.data[repo_id]
        else:
            start = 0

        for i, row in enumerate(dataset):
            if i < start:
                continue

            print(f'{i + 1}/{len(dataset)}')

            upload_document(
                document_id=repo_id.replace('/', '--') + f'-{i}',
                repo='huggingface:' + repo_id,
                url=row['url'],
                title=row['title'],
                content=row['content'],
            )

            s.data[repo_id] = i + 1


if __name__ == '__main__':
    main()
