# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os
import logging
import subprocess
from datetime import datetime
from sqlalchemy.event import listen
from sqlalchemy import event
from sqlalchemy.engine import Engine
from flask_appbuilder import AppBuilder  # Import AppBuilder here
from flask import g

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)
logger.debug("PLUGIN: Plugin file loaded")
print("PLUGIN: Plugin file loaded")

QUERY_EXPORT_DIR = '/app/Ntherm-DW/db/mssql-ntherm/superset-data/queries'
DATASET_EXPORT_DIR = '/app/Ntherm-DW/db/mssql-ntherm/superset-data/datasets'

def export_query_to_file(query):
    # Ensure the directory exists
    try:
        os.makedirs(QUERY_EXPORT_DIR, exist_ok=True)
    except Exception as e:
        logger.error(f"Failed to create export directory: {e}")
        return

    # Format the file name
    filename = f"{query.id}_{query.label}.sql"
    file_path = os.path.join(QUERY_EXPORT_DIR, filename)

    # Write the query to the file
    try:
        with open(file_path, "w") as file:
            file.write(query.sql)
    except Exception as e:
        logger.error(f"PLUGIN: Failed to write query to file: {e}")

def on_query_saved(mapper, connection, target):
    export_query_to_file(target)
    git_sync(commit_message=f"Auto commit from on_query_saved-{datetime.now()}")

def on_query_deleted(mapper, connection, target):
    filename = f"{target.id}_{target.label}.sql"
    file_path = os.path.join(QUERY_EXPORT_DIR, filename)
    try:
        os.remove(file_path)
    except OSError:
        pass

def export_dataset_to_file(dataset):
    filename = f"{dataset.table_name}.sql"
    file_path = os.path.join(DATASET_EXPORT_DIR, filename)

    print(f"PLUGIN: schema-{dataset.schema}")
    if dataset.schema not in ["public", "main"]:
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "w") as file:
                file.write(dataset.sql)
        except Exception as e:
            logger.error(f"PLUGIN: Failed to write dataset to file: {e}")

def git_sync(commit_message=None):
    repo_path = "/app/Ntherm-DW/db/mssql-ntherm/superset-data"
    ssh_config_path = "/mnt/ssh/config"
    env = os.environ.copy()
    env["GIT_SSH_COMMAND"] = f"ssh -F {ssh_config_path}"

    # Ensure the directory exists
    try:
        os.makedirs(QUERY_EXPORT_DIR, exist_ok=True)
        os.makedirs(DATASET_EXPORT_DIR, exist_ok=True)

        # Mark the directory as safe
        subprocess.run(["git", "config", "--global", "--add", "safe.directory", "/app/Ntherm-DW"], check=True)

    except Exception as e:
        logger.error(f"Failed to create export directory: {e}")
        return

    if not commit_message:
        commit_message = f"Auto commit from Superset on {datetime.now().isoformat()}"

    try:
        # Stage changes
        subprocess.run(["git", "add", "."], cwd=repo_path, check=True, env=env)

        # Commit
        subprocess.run(["git", "commit", "-m", commit_message], cwd=repo_path, check=True, env=env)

        # Pull remote changes
        subprocess.run(["git", "pull", "--rebase"], cwd=repo_path, check=True, env=env)

        # Push
        subprocess.run(["git", "push"], cwd=repo_path, check=True, env=env)

        print("✅ Git sync completed successfully.")

    except subprocess.CalledProcessError as e:
        print(f"❌ Git command failed: {e}")
        print(f"🔁 Command: {e.cmd}")
        if e.stdout:
            print(f"📤 STDOUT:\n{e.stdout.decode()}")
        if e.stderr:
            print(f"📥 STDERR:\n{e.stderr.decode()}")

def dataset_saved_listener(mapper, connection, target):
    export_dataset_to_file(target)
    git_sync(commit_message=f"Auto commit from dataset_saved_listener-{datetime.now()}")

def dataset_deleted_listener(mapper, connection, target):
    filename = f"{target.table_name}.sql"
    file_path = os.path.join(DATASET_EXPORT_DIR, filename)
    logger.debug(f"PLUGIN: dataset_deleted_listener-{file_path}")    
    print(f"PLUGIN: dataset_deleted_listener-{file_path}")
    if target.schema not in ["public", "main"]:
        try:
            os.remove(file_path)
        except Exception as e:
            logger.error(f"PLUGIN: Failed to delete dataset to file: {e}")

class SaveQueryExportPlugin:
    def __init__(self, appbuilder: AppBuilder):
        self.appbuilder = appbuilder

    def init_app(self):
        """This method is called from app.py"""
        logger.debug("PLUGIN: Initializing plugin within app context")
        self.register_views()

    def register_views(self):
        from superset.models.sql_lab import SavedQuery
        from superset.connectors.sqla.models import SqlaTable as Dataset

        listen(SavedQuery, "after_insert", on_query_saved)
        listen(SavedQuery, "after_update", on_query_saved)
        listen(SavedQuery, "after_delete", on_query_deleted)
        event.listen(Dataset, "after_insert", dataset_saved_listener)
        event.listen(Dataset, "after_update", dataset_saved_listener)
        event.listen(Dataset, "after_delete", dataset_deleted_listener)