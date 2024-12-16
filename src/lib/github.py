import logging
from typing import Dict, Optional, Tuple

import requests
from flask import current_app

logger = logging.getLogger(__name__)

GH_BASE_URL = 'https://github.com'
GH_API_BASE_URL = 'https://api.github.com'

COMMONWL_BASE_URL = 'https://view.commonwl.org/workflows'


def _get_headers() -> Dict[str, str]:
    return {
        'Authorization': f'Bearer {current_app.config["GITHUB_API_TOKEN"]}',
        'Accept': 'application/vnd.github+json',
        'X-GitHub-Api-Version': '2022-11-28',
    }


def parse_repo_name(url: str) -> Optional[Tuple[str, str]]:
    if url is None or len(url) == 0:
        return None

    url = url.removeprefix(f'{GH_BASE_URL}/')
    url = url.removesuffix('.git')
    match = url.split('/')
    if match is None or len(match) != 2:
        logging.error(f'Invalid GitHub repository name: {url}')
        return None
    return tuple(match)


def get_repo_description(owner: str, repo: str) -> Optional[str]:
    gh_url = f'{GH_API_BASE_URL}/repos/{owner}/{repo}'
    res = requests.get(
        gh_url,
        headers=_get_headers(),
    )
    if res.status_code != 200:
        logging.error(f'Failed to get GitHub repository description: {res.status_code}, {res.text}')
        return None
    return res.json()['description']


def get_complete_hash(owner: str, repo: str, commit: str) -> Optional[str]:
    gh_url = f'{GH_API_BASE_URL}/repos/{owner}/{repo}/commits/{commit}'
    res = requests.get(
        gh_url,
        headers=_get_headers(),
    )
    if res.status_code != 200:
        logging.error(f'Failed to get GitHub complete hash: {res.status_code}, {res.text}')
        return None
    return res.json()['sha']


def get_tags(owner: str, repo: str) -> Optional[Dict[str, str]]:
    gh_url = f'{GH_API_BASE_URL}/repos/{owner}/{repo}/tags'
    res = requests.get(
        gh_url,
        headers=_get_headers(),
    )
    if res.status_code != 200:
        logging.error(f'Failed to get GitHub tags: {res.status_code}, {res.text}')
        return None
    return {tag['commit']['sha'][:7]: tag['name'] for tag in res.json()}


def get_tag(owner: str, repo: str, hash: str) -> Optional[str]:
    tags = get_tags(owner, repo)
    if tags is None:
        logging.error(f'Failed to get GitHub tag: {owner}/{repo}/{hash}')
        return None
    return tags.get(hash[:7])


def create_commit_url(owner: str, repo: str, commit: str) -> Optional[str]:
    if len(commit) < 40:
        commit = get_complete_hash(owner, repo, commit)

    if commit is None:
        logging.error(f'Failed to create GitHub commit URL: {owner}/{repo}/{commit}')
        return None

    return f'https://github.com/{owner}/{repo}/tree/{commit}'


def create_tag_url(owner: str, repo: str, tag: str) -> str:
    return f'{GH_BASE_URL}/{owner}/{repo}/releases/tag/{tag}'


def create_commonwl_url(owner: str, repo: str, commit: str, filename: str) -> str:
    if len(commit) < 40:
        cmp_commit = get_complete_hash(owner, repo, commit)
        if cmp_commit is not None:
            commit = cmp_commit
    return f'{COMMONWL_BASE_URL}/github.com/{owner}/{repo}/blob/{commit}/{filename}'
