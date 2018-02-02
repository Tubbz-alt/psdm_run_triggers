'''
The model level business logic goes here.
Most of the code here gets a connection to the database, executes a query and formats the results.
'''

import json

from context import logbook_db

from dal.sql_queries import QUERY_SELECT_JOB_HASHES_FOR_EXPERIMENT, QUERY_INSERT_JOB_HASH_FOR_EXPERIMENT

__author__ = 'mshankar@slac.stanford.edu'


def get_current_job_hashes(experiment_id):
    """
    Get the current job hashes for the given experiment
    :param experiment_id
    :return: List of batch job hashes for this instrument.
    """
    with logbook_db.connect() as cursor:
        cursor.execute(QUERY_SELECT_JOB_HASHES_FOR_EXPERIMENT, {"experiment_id": experiment_id})
        return cursor.fetchall()

def register_new_job_hash(experiment_id, job_details):
    """
    Add a new hash into the batch_hashes for the specified experiment
    """
    jc = {"experiment_id": experiment_id}
    jc.update(job_details)
    with logbook_db.connect() as cursor:
        cursor.execute(QUERY_INSERT_JOB_HASH_FOR_EXPERIMENT, jc)
        return cursor.lastrowid
