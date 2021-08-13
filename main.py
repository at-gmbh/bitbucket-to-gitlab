import os
import time
from abc import ABC, abstractmethod
from pathlib import PurePosixPath
from typing import Dict, Iterable, List, NamedTuple, Optional, Union

from atlassian import Bitbucket
from gitlab import DEVELOPER_ACCESS, Gitlab, GitlabError, GitlabHttpError, \
    OWNER_ACCESS, REPORTER_ACCESS
from gitlab.v4.objects import Group, Project, User
from tqdm import tqdm

# -----------------------------------------------------------------------------
# please provide credentials through these environment variables
# -----------------------------------------------------------------------------

GITLAB_URL = os.getenv('GITLAB_URL')
GITLAB_TOKEN = os.getenv('GITLAB_TOKEN')
BITBUCKET_URL = os.getenv('BITBUCKET_URL')
BITBUCKET_USER = os.getenv('BITBUCKET_USER')
BITBUCKET_TOKEN = os.getenv('BITBUCKET_TOKEN')

# -----------------------------------------------------------------------------
# please change these config options according to your needs
# -----------------------------------------------------------------------------

# how to handle duplicates. one of 'error' (raise an exception), 'ignore' (don't import),
# 'rename' (import under a different name)
on_duplicate = 'ignore'
# common prefix to add before all groups. supports subgroups like namespace/subgroup (Optional)
group_prefix = ''
# the max. number of imports to run at the same time
# (one import per CPU core on your GitLab server should work fine)
parallel_imports = 4
# don't import projects with these Bitbucket project keys (optional)
project_blacklist = []
# map bitbucket permissions to these gitlab access levels
permission_map = {
    'PROJECT_READ': REPORTER_ACCESS,
    'REPO_READ': REPORTER_ACCESS,
    'PROJECT_WRITE': DEVELOPER_ACCESS,
    'REPO_WRITE': DEVELOPER_ACCESS,
    'PROJECT_ADMIN': OWNER_ACCESS,
    'REPO_ADMIN': OWNER_ACCESS,
}

# -----------------------------------------------------------------------------


class ProjectMapping(NamedTuple):
    bb_project: str
    """the Bitbucket project name (in GitLab: group name)"""
    bb_repo: str
    """the Bitbucket repository name (in GitLab: project name)"""
    gl_group: str
    """the new group name in GitLab (may point to a subgroup, e.g. "group/subgroup")"""
    gl_project: str
    """the new project slug in GitLab (under which URL the project will be accessible)"""

    @property
    def gitlab_path(self):
        return f"{self.gl_group}/{self.gl_project}"


class BitbucketRepoGenerator(ABC):

    def __init__(self):
        # check params
        check_env('BITBUCKET_URL')
        check_env('BITBUCKET_USER')
        check_env('BITBUCKET_TOKEN')
        # connect to bitbucket
        self.bitbucket = Bitbucket(
            url=BITBUCKET_URL, username=BITBUCKET_USER, password=BITBUCKET_TOKEN)
        self.group_count: Optional[int] = None

    @abstractmethod
    def yield_repos(self) -> Iterable[ProjectMapping]:
        pass


class BitbucketMainRepoGenerator(BitbucketRepoGenerator):

    def __init__(self):
        super().__init__()
        print(f"requesting all repos from {BITBUCKET_URL} that are visible to {BITBUCKET_USER}")
        self.projects = list(self.bitbucket.project_list())
        self.group_count = len(self.projects)

    def yield_repos(self):
        # iterate over all projects (groups) and repos (projects) in bitbucket
        counter = 0
        for bb_project in tqdm(self.projects, unit='project groups'):
            bb_project_slug = bb_project['key']
            if bb_project_slug in project_blacklist:
                continue
            gl_group = get_gitlab_group(bb_project_slug)
            # list all repos in this group
            for bb_repo in self.bitbucket.repo_list(bb_project_slug):
                bb_repo_slug = bb_repo['slug']
                project = ProjectMapping(
                    bb_project=bb_project_slug,
                    bb_repo=bb_repo_slug,
                    gl_group=gl_group,
                    gl_project=bb_repo_slug,
                )
                yield project
                counter += 1
        tqdm.write(f"{counter} Bitbucket repos have been returned")


class BitbucketPersonalRepoGenerator(BitbucketRepoGenerator):

    def __init__(self):
        super().__init__()
        print(f"requesting all users from {BITBUCKET_URL} that are visible to {BITBUCKET_USER}")
        self.users = list(self.bitbucket.get_users_info(limit=None))
        self.group_count = len(self.users)

    def yield_repos(self) -> Iterable[ProjectMapping]:
        counter = 0
        for bb_user in tqdm(self.users, unit='users'):
            bb_user_slug = bb_user['slug']
            if bb_user_slug in project_blacklist:
                continue
            bb_user_path = f'~{bb_user_slug}'
            # list all repos in this group
            bb_repos = list(self.bitbucket.repo_list(bb_user_path))
            if not bb_repos:
                tqdm.write(f"skipping {bb_user_slug}, no personal projects found")
            for bb_repo in bb_repos:
                bb_repo_slug = bb_repo['slug']
                project = ProjectMapping(
                    bb_project=bb_user_path,
                    bb_repo=bb_repo_slug,
                    gl_group=bb_user_slug,
                    gl_project=bb_repo_slug,
                )
                yield project
                counter += 1
        tqdm.write(f"{counter} Bitbucket repos have been returned")


def check_env(env: str):
    if not os.getenv(env):
        raise ValueError(f"please provide {env} as environment variable")


def get_gitlab_group(bitbucket_project):
    if group_prefix:
        return str(PurePosixPath(group_prefix.strip('/')) / bitbucket_project)
    else:
        return bitbucket_project


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
        bb_project_slug = bb_project['key']
        tqdm.write(f"----- {bb_project_slug} -----")

        # skip when blacklisted
        if bb_project_slug in project_blacklist:
            tqdm.write(f"skipping blacklisted project {bb_project_slug}")
            continue

        # skip when there are no repos
        bb_repo_list = list(bitbucket.repo_list(bb_project_slug))
        if not bb_repo_list:
            tqdm.write(f"skipping empty project {bb_project_slug}")
            continue

        # copy group permissions
        bb_project_users = list(bitbucket.project_users(bb_project_slug))
        gl_group_path = get_gitlab_group(bb_project_slug)
        gl_group = gitlab.groups.get(gl_group_path)
        copy_permissions_for(
            gitlab, user_map, bb_project_users, gl_group, current_user, dry_run=dry_run)

        # copy project permissions
        for bb_repo in bb_repo_list:
            repo_slug = bb_repo['slug']
            bb_repo_users = list(bitbucket.repo_users(bb_project_slug, repo_slug))
            gl_project = gitlab.projects.get(f'{gl_group_path}/{repo_slug}')
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


def import_main_projects():
    repo_generator = BitbucketMainRepoGenerator()
    import_projects(repo_generator)


def import_personal_projects():
    repo_generator = BitbucketPersonalRepoGenerator()
    import_projects(repo_generator)


def import_projects(repo_generator: BitbucketRepoGenerator):
    # import all projects
    print(f"importing {repo_generator.group_count} project groups in GitLab at {GITLAB_URL}")
    gitlab = Gitlab(GITLAB_URL, private_token=GITLAB_TOKEN)
    projects_iter = repo_generator.yield_repos()
    processing: List[Project] = []
    counter = 0
    # imports are running asynchronously and in parallel. we frequently check the status
    # of each import and queue new imports until we run out of jobs to process
    while True:
        if len(processing) < parallel_imports:
            try:
                project: ProjectMapping = next(projects_iter)
                tqdm.write(f"importing {project.gitlab_path}")
                job = trigger_import(gitlab, project)
                if job:
                    processing.append(job)
                    counter += 1
            except StopIteration:
                tqdm.write(f"all imports were triggered, waiting for running jobs to finish")
                break
        else:
            processing = check_and_sleep(gitlab, processing)
    # almost finished, just wait for the last few jobs
    while processing:
        processing = check_and_sleep(gitlab, processing)
    print(f"{counter} projects were imported in GitLab")


def check_and_sleep(gitlab: Gitlab, processing: List[Project], sleep_time=1.0) -> List[Project]:
    updated = []
    for job in processing:
        status = gitlab.projects.get(job.id)
        if status.import_status == 'started':
            updated.append(status)
        else:
            if status.import_status == 'finished':
                tqdm.write(f"import of {status.path_with_namespace} finished successfully")
            else:
                tqdm.write(f"warning: import of {status.path_with_namespace} finished "
                           f"with status {status.import_status}")
    if len(updated) >= parallel_imports:
        time.sleep(sleep_time)
    return updated


def trigger_import(gitlab: Gitlab, project: ProjectMapping) -> Optional[Project]:
    if on_duplicate == 'error':
        return _trigger_import(gitlab, project)
    elif on_duplicate in ('ignore', 'rename'):
        try:
            return _trigger_import(gitlab, project)
        except GitlabHttpError as e:
            if e.response_code == 422 and "Path has already been taken" in str(e):
                if on_duplicate == 'ignore':
                    tqdm.write(f"repo {project.gitlab_path} already exists, skipping")
                elif on_duplicate == 'rename':
                    # TODO find a way to try suffixes until it works...
                    tqdm.write(f"repo {project.gitlab_path} already exists, renaming")
                    return _trigger_import(gitlab, project, suffix="_BB")
            else:
                print(f"there was an unexpected error while importing {project}. {e}")
                raise e
    else:
        raise ValueError(f"unexpected value {on_duplicate} for on_duplicate")


def _trigger_import(gitlab: Gitlab, project: ProjectMapping, suffix: str = None) -> Project:
    # define the namespace
    gl_project_slug = project.gl_project
    if suffix:
        gl_project_slug += suffix
    # start the import process
    result = gitlab.projects.import_bitbucket_server(
        bitbucket_server_url=BITBUCKET_URL,
        bitbucket_server_username=BITBUCKET_USER,
        personal_access_token=BITBUCKET_TOKEN,
        bitbucket_server_project=project.bb_project,
        bitbucket_server_repo=project.bb_repo,
        new_name=gl_project_slug,
        target_namespace=project.gl_group,
    )
    job = gitlab.projects.get(result['id'])
    return job


def main():
    # import all projects in the main namespace
    print("== importing Bitbucket projects from the main namespace ==")
    import_main_projects()
    # now we copy all permissions (these are not covered by the gitlab import)
    print("== copying members and permissions for all projects that were migrated ==")
    copy_permissions()
    # import all personal projects (permissions are set correctly here)
    print("== importing Bitbucket projects from the user namespace ==")
    import_personal_projects()


if __name__ == '__main__':
    main()
