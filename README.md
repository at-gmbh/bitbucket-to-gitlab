# Bitbucket to GitLab

A Python script to migrate projects from Bitbucket (Server/Cloud) to GitLab.

Reads metadata from BitBucket via the [Atlassian Python API](https://atlassian-python-api.readthedocs.io/) and imports projects to GitLab using [python-gitlab](https://atlassian-python-api.readthedocs.io/). Project and repo names in Bitbucket are converted to group names and projects in GitLab, therefore the structure of the projects should be preserved. Projects are imported using the [GitLab Import API](https://docs.gitlab.com/ee/api/import.html), which comes with [a few limitations](https://docs.gitlab.com/ee/user/project/import/bitbucket_server.html#limitations), but at least projects, metadata, pull requests and user mappings are preserved.

## Migrating Data

Prerequisites

* a Bitbucket account which can access all projects you want to migrate (preferably an admin account)
* a GitLab account that has permission to create groups and projects (preferably an admin account)
* create a [personal access token](https://docs.gitlab.com/ee/user/profile/personal_access_tokens.html) for your GitLab account and give it at least the "api" scope (complete read/write access to the API)
* optional: create a [personal access token](https://confluence.atlassian.com/bitbucketserver/personal-access-tokens-939515499.html) for Bitbucket (otherwise you may use your password)

Setup

* make sure that Python 3.7 or higher is installed on your system (`python --version`) and [update, if necessary](https://www.python.org/downloads/)
* clone this repo: `git clone https://github.com/at-gmbh/bitbucket-to-gitlab.git`
* install dependencies: `pip install -r requirements.txt`

Run

* Define the necessary environment variables
  - `BITBUCKET_URL`: The URL of your Bitbucket instance
  - `BITBUCKET_USER`: Your user name
  - `BITBUCKET_TOKEN`: Your personal access token or your password
  - `GITLAB_URL`: The URL of your GitLab instance
  - `GITLAB_TOKEN`: Your personal access token (those are always unique, therefore no user name is required)
* optional: Adjust the config in `main.py` (`skip_existing`, `group_prefix`, `parallel_imports`)
* run the script: `python main.py`

## License

    Copyright 2021 Alexander Thamm GmbH

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
