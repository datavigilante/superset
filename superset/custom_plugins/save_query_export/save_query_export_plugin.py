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

QUERY_EXPORT_DIR = '/app/superset/custom_plugins/save_query_export/queries'
DATASET_EXPORT_DIR = '/app/superset/custom_plugins/save_query_export/datasets'

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

    if dataset.schema_perm != "[examples].[public]":
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "w") as file:
                file.write(dataset.sql)
        except Exception as e:
            logger.error(f"PLUGIN: Failed to write dataset to file: {e}")

def dataset_saved_listener(mapper, connection, target):
    export_dataset_to_file(target)

def dataset_deleted_listener(mapper, connection, target):
    filename = f"{target.table_name}.sql"
    file_path = os.path.join(DATASET_EXPORT_DIR, filename)
    logger.debug(f"PLUGIN: dataset_deleted_listener-{file_path}")    
    print(f"PLUGIN: dataset_deleted_listener-{file_path}")
    os.remove(file_path)

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