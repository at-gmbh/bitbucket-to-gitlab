import os
import time
from pathlib import PurePosixPath
from typing import Dict, List, NamedTuple, Optional, Union

from atlassian import Bitbucket
from gitlab import DEVELOPER_ACCESS, Gitlab, GitlabError, GitlabHttpError, \
    OWNER_ACCESS, REPORTER_ACCESS
from gitlab.v4.objects import Group, Project, User
from tqdm import tqdm

# please provide credentials through these environment variables
GITLAB_URL = os.getenv('GITLAB_URL')
GITLAB_TOKEN = os.getenv('GITLAB_TOKEN')
BITBUCKET_URL = os.getenv('BITBUCKET_URL')
BITBUCKET_USER = os.getenv('BITBUCKET_USER')
BITBUCKET_TOKEN = os.getenv('BITBUCKET_TOKEN')

# config options

# how to handle duplicates. one of 'error' (raise an exception), 'ignore' (don't import),
# 'rename' (import under a different name)
on_duplicate = 'rename'
# common prefix to add before all groups. supports subgroups like namespace/subgroup (Optional)
group_prefix = ''
# the max. number of imports to run at the same time
# (one import per CPU core on your GitLab server should work fine)
parallel_imports = 4
# don't import projects with this project slug
project_blacklist = ['ATTS', 'KUB', 'TEC', 'TECHDM']
# map bitbucket permissions to these gitlab access levels
permission_map = {
    'PROJECT_READ': REPORTER_ACCESS,
    'REPO_READ': REPORTER_ACCESS,
    'PROJECT_WRITE': DEVELOPER_ACCESS,
    'REPO_WRITE': DEVELOPER_ACCESS,
    'PROJECT_ADMIN': OWNER_ACCESS,
    'REPO_ADMIN': OWNER_ACCESS,
}


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
        if bb_project in project_blacklist:
            continue
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
        break   # TODO remove
    print(f"finished, returning {len(repos)} projects")
    return repos


def copy_permissions(dry_run=False):
    # prepare bitbucket & gitlab
    bitbucket = Bitbucket(url=BITBUCKET_URL, username=BITBUCKET_USER, password=BITBUCKET_TOKEN)
    gitlab = Gitlab(GITLAB_URL, private_token=GITLAB_TOKEN)
    user_map = {}
    gitlab.auth()
    current_user = gitlab.user

    # go through all bitbucket projects
    project_list = list(bitbucket.project_list())
    for bb_project in tqdm(project_list, unit='project'):
        project_slug = bb_project['key']
        tqdm.write(f"----- {project_slug} -----")

        # skip when blacklisted
        if project_slug in project_blacklist:
            tqdm.write(f"skipping blacklisted project {project_slug}")
            continue

        # skip when there are no repos
        repo_list = list(bitbucket.repo_list(project_slug))
        if not repo_list:
            tqdm.write(f"skipping empty project {project_slug}")
            continue

        # copy group permissions
        bb_project_users = list(bitbucket.project_users(project_slug))
        gl_group = gitlab.groups.get(project_slug)
        copy_permissions_for(
            gitlab, user_map, bb_project_users, gl_group, current_user, dry_run=dry_run)

        # copy project permissions
        for bb_repo in repo_list:
            repo_slug = bb_repo['slug']
            bb_repo_users = list(bitbucket.repo_users(project_slug, repo_slug))
            gl_project = gitlab.projects.get(f'{project_slug}/{repo_slug}')
            copy_permissions_for(
                gitlab, user_map, bb_repo_users, gl_project, current_user, dry_run=dry_run)

    print("finished fixing permissions")


def copy_permissions_for(gitlab: Gitlab, user_map: Dict[str, User], bb_users: List[Dict],
                         gl_entity: Union[Group, Project], current_user: User, dry_run=False):
    # break early if there are no users
    entity_type = type(gl_entity).__name__
    if not bb_users:
        tqdm.write(f"no permissions to copy for {entity_type} {gl_entity.path}")
        return

    # try to map permissions for all users
    users_granted = {}
    for bb_user in bb_users:
        bb_user_name = bb_user['user']['slug']
        bb_user_access = bb_user['permission']
        if bb_user_name not in user_map:
            response = gitlab.users.list(username=bb_user_name)
            user_map[bb_user_name] = response[0] if response else None
        gl_user = user_map[bb_user_name]
        gl_user_access = permission_map[bb_user_access]
        if gl_user:
            users_granted[gl_user.username] = gl_user_access
            tqdm.write(f"adding {gl_user.username} to {entity_type} {gl_entity.path} as {bb_user_access}")
            if not dry_run:
                try:
                    gl_entity.members.create({'user_id': gl_user.id, 'access_level': gl_user_access})
                except GitlabError as e:
                    try:
                        gl_entity.members.create({'user_id': gl_user.id, 'access_level': gl_user_access - 10})
                    except GitlabError as e:
                        if "already exists" in str(e):
                            tqdm.write(f"user {gl_user.username} already exists in {entity_type} {gl_entity.path}")
                        elif "inherited membership from group" in str(e):
                            tqdm.write(f"ignoring lower access to {entity_type} {gl_entity.path} for {gl_user.username}")
                        else:
                            tqdm.write(f"failed to add {gl_user.username} to {entity_type} {gl_entity.path}: {e}")

    # remove the current user, if someone else was added as admin
    admin_added = any(level >= 50 for level in users_granted.values())
    if admin_added:
        tqdm.write(f"deleting {current_user.username} from {entity_type} {gl_entity.path}")
        if not dry_run:
            try:
                gl_entity.members.delete(current_user.id)
            except GitlabError as e:
                if "404" not in str(e):
                    tqdm.write(f"failed to delete {current_user.username} from {entity_type} {gl_entity.path}: {e}")
    else:
        tqdm.write(f"no new owner was added to {gl_entity.path}, keeping {current_user.username} as owner")


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
    if on_duplicate == 'error':
        return _trigger_import(gitlab, project)
    elif on_duplicate in ('ignore', 'rename'):
        try:
            return _trigger_import(gitlab, project)
        except GitlabHttpError as e:
            if e.response_code == 422 and "Path has already been taken" in str(e):
                if on_duplicate == 'ignore':
                    tqdm.write(f"repo {project.path} already exists, skipping")
                elif on_duplicate == 'rename':
                    # TODO find a way to try suffixes until it works...
                    tqdm.write(f"repo {project.path} already exists, renaming")
                    return _trigger_import(gitlab, project, suffix="_BB")
            else:
                print(f"there was an unexpected error while importing {project}. {e}")
                raise e
    else:
        raise ValueError(f"unexpected value {on_duplicate} for on_duplicate")


def _trigger_import(gitlab: Gitlab, project: BitBucketRepo, suffix: str = None) -> Project:
    # define the namespace
    if group_prefix:
        target_path = str(PurePosixPath(group_prefix.strip('/')) / project.project_slug)
    else:
        target_path = project.project_slug
    project_slug = project.repo_slug
    if suffix:
        project_slug += suffix
    # start the import process
    result = gitlab.projects.import_bitbucket_server(
        bitbucket_server_url=BITBUCKET_URL,
        bitbucket_server_username=BITBUCKET_USER,
        personal_access_token=BITBUCKET_TOKEN,
        bitbucket_server_project=project.project_slug,
        bitbucket_server_repo=project.repo_slug,
        new_name=project_slug,
        target_namespace=target_path,
    )
    job = gitlab.projects.get(result['id'])
    return job


def main():

    # get all projects from your bitbucket server
    projects = get_bitbucket_repos()
    # now would be a good time to filter the projects, otherwise we'll migrate everything
    import_projects(projects)
    # now we copy all permissions (these are not covered by the gitlab import)
    copy_permissions(dry_run=False)
    # TODO copy personal projects


if __name__ == '__main__':
    main()
