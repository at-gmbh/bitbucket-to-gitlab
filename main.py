import os
import time

from atlassian import Bitbucket
from gitlab import Gitlab

GITLAB_URL = os.getenv('GITLAB_URL')
GITLAB_TOKEN = os.getenv('GITLAB_TOKEN')
BITBUCKET_URL = os.getenv('BITBUCKET_URL')
BITBUCKET_USER = os.getenv('BITBUCKET_USER')
BITBUCKET_TOKEN = os.getenv('BITBUCKET_TOKEN')


def bitbucket_foo():
    if not BITBUCKET_URL:
        raise ValueError("...")
        # user, token
    bitbucket = Bitbucket(
        url=BITBUCKET_URL,
        username=BITBUCKET_USER,
        password=BITBUCKET_TOKEN)
    data = list(bitbucket.project_list())
    foo = [{'group_name': d['name'], 'group_slug': d['key']} for d in data]
    repos = list(bitbucket.repo_list('AC'))
    print(data)


def gitlab_foo():
    gl = Gitlab(GITLAB_URL, private_token=GITLAB_TOKEN)
    projects = gl.projects.list()

    pass


if __name__ == '__main__':
    #bitbucket_foo()
    gitlab_foo()
    pass
