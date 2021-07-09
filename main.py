import os
import time
from pathlib import PurePosixPath
from typing import List, NamedTuple, Optional

from atlassian import Bitbucket
from gitlab import Gitlab, GitlabHttpError
from gitlab.v4.objects import Project
from tqdm import tqdm

# please provide credentials through these environment variables
GITLAB_URL = os.getenv('GITLAB_URL')
GITLAB_TOKEN = os.getenv('GITLAB_TOKEN')
BITBUCKET_URL = os.getenv('BITBUCKET_URL')
BITBUCKET_USER = os.getenv('BITBUCKET_USER')
BITBUCKET_TOKEN = os.getenv('BITBUCKET_TOKEN')

# config options
skip_existing = True
# common prefix to add before all groups. supports subgroups like namespace/subgroup (Optional)
group_prefix = 'bitbucket'
# the max. number of imports to run at the same time
# (one import per CPU core on your GitLab server should work fine)
parallel_imports = 4


class BitBucketRepo(NamedTuple):
    project_name: str
    """the project name (in GitLab: group name)"""
    project_slug: str
    """the project slug (in GitLab: group slug)"""
    repo_name: str
    """the repo name (in GitLab: project name)"""
    repo_slug: str
    """the repo slug (in GitLab: project slug)"""

    @property
    def path(self):
        return f"{self.project_slug}/{self.repo_name}"


def check_env(env: str):
    if not os.getenv(env):
        raise ValueError(f"please provide {env} as environment variable")


def get_bitbucket_repos() -> List[BitBucketRepo]:
    # check params
    check_env('BITBUCKET_URL')
    check_env('BITBUCKET_USER')
    check_env('BITBUCKET_TOKEN')
    # iterate over all projects (groups) and repos (projects) in bitbucket
    print(f"requesting all repos from {BITBUCKET_URL} that are visible to {BITBUCKET_USER}")
    bitbucket = Bitbucket(url=BITBUCKET_URL, username=BITBUCKET_USER, password=BITBUCKET_TOKEN)
    project_list = list(bitbucket.project_list())
    repos = []
    for bb_project in tqdm(project_list, unit='project'):
        project_name = bb_project['name']
        project_slug = bb_project['key']
        for bb_repo in bitbucket.repo_list(project_slug):
            repo = BitBucketRepo(
                project_name=project_name,
                project_slug=project_slug,
                repo_name=bb_repo['name'],
                repo_slug=bb_repo['slug'],
            )
            repos.append(repo)
    print(f"finished, returning {len(repos)} projects")
    return repos


def import_projects(projects: List[BitBucketRepo]):
    # check params
    check_env('GITLAB_URL')
    check_env('GITLAB_TOKEN')
    # import all projects
    print(f"importing {len(projects)} projects in GitLab at {GITLAB_URL}")
    gitlab = Gitlab(GITLAB_URL, private_token=GITLAB_TOKEN)
    projects_iter = iter(projects)
    processing: List[Project] = []
    counter = 0
    # imports are running asynchronously and in parallel. we frequently check the status
    # of each import and queue new imports until we run out of jobs to process
    with tqdm(total=len(projects), unit='import') as pbar:
        while True:
            if len(processing) < parallel_imports:
                try:
                    project = next(projects_iter)
                    tqdm.write(f"importing {project.path}")
                    job = trigger_import(gitlab, project)
                    if job:
                        processing.append(job)
                        counter += 1
                    else:
                        pbar.update(1)
                except StopIteration:
                    tqdm.write(f"all imports were triggered, waiting for running jobs to finish")
                    break
            else:
                processing = check_and_sleep(gitlab, processing, pbar)
        # almost finished, just wait for the last few jobs
        while processing:
            processing = check_and_sleep(gitlab, processing, pbar)
    print(f"{counter} projects were imported in GitLab")


def check_and_sleep(gitlab: Gitlab, processing: List[Project], pbar: tqdm,
                    sleep_time=1.0) -> List[Project]:
    updated = []
    for job in processing:
        status = gitlab.projects.get(job.id)
        if status.import_status == 'started':
            updated.append(status)
        else:
            pbar.update(1)
            if status.import_status == 'finished':
                tqdm.write(f"import of {status.path_with_namespace} finished successfully")
            else:
                tqdm.write(f"warning: import of {status.path_with_namespace} finished "
                           f"with status {status.import_status}")
    if len(updated) >= parallel_imports:
        time.sleep(sleep_time)
    return updated


def trigger_import(gitlab: Gitlab, project: BitBucketRepo) -> Optional[Project]:
    if skip_existing:
        try:
            return _trigger_import(gitlab, project)
        except GitlabHttpError as e:
            if e.response_code == 422 and "Path has already been taken" in str(e):
                tqdm.write(f"repo {project.path} already exists, skipping")
            else:
                raise e
    else:
        return _trigger_import(gitlab, project)


def _trigger_import(gitlab: Gitlab, project: BitBucketRepo) -> Project:
    # define the namespace
    if group_prefix:
        target_path = str(PurePosixPath(group_prefix.strip('/')) / project.project_slug)
    else:
        target_path = project.project_slug
    # start the import process
    result = gitlab.projects.import_bitbucket_server(
        bitbucket_server_url=BITBUCKET_URL,
        bitbucket_server_username=BITBUCKET_USER,
        personal_access_token=BITBUCKET_TOKEN,
        bitbucket_server_project=project.project_slug,
        bitbucket_server_repo=project.repo_slug,
        target_namespace=target_path
    )
    job = gitlab.projects.get(result['id'])
    return job


def main():
    # get all projects from your bitbucket server
    projects = get_bitbucket_repos()
    # now would be a good time to filter the projects, otherwise we'll migrate everything
    import_projects(projects)


if __name__ == '__main__':
    main()
