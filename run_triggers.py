from flask import Flask, current_app
import logging
import os
import sys
import json
from kafka import KafkaConsumer, TopicPartition
from threading import Thread
import eventlet
import requests

from context import app, logbook_db

from pages import pages_blueprint

from dal.autorun import get_current_job_hashes, register_new_job_hash


__author__ = 'mshankar@slac.stanford.edu'

logger = logging.getLogger(__name__)

app = Flask("psdm_run_triggers")
# Set the expiration for static files
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 300;

app.secret_key = "This is a secret key that is somewhat temporary."
app.debug = bool(os.environ.get('DEBUG', "False"))

if app.debug:
    print("Sending all debug messages to the console")
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    logging.getLogger('kafka').setLevel(logging.INFO)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    root.addHandler(ch)


# Register routes.
app.register_blueprint(pages_blueprint, url_prefix="")
logbook_db.init_app(app)

def load_default_auto_runs():
    with open('default_auto_runs.json', 'r') as f:
        default_auto_runs = json.load(f)
        return default_auto_runs


def insert_autorun_entries():
    consumer = KafkaConsumer('runs.start', bootstrap_servers=[os.environ.get("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")])

    while True:
        msg = next(consumer)

        if msg:
            logger.info("Message from Kafka %s", msg.value)
            info = json.loads(msg.value)
            exp_id = info['experiment_id']
            run_id = info['run_id']

            default_auto_runs = load_default_auto_runs()
            auto_job_names = { x['hash'] for x in default_auto_runs }
            name2job = {x['hash'] : x for x in default_auto_runs }
            already_registered_jobs = { x['hash'] for x in get_current_job_hashes(exp_id)}
            for unregistered_job in auto_job_names - already_registered_jobs:
                logger.info("Adding job hash %s for experiment %s ", unregistered_job, exp_id)
                job_details = name2job[unregistered_job]
                register_new_job_hash(exp_id, job_details)


# Create thread for autorun
autorun_thread = Thread(target=insert_autorun_entries)
autorun_thread.start()



if __name__ == '__main__':
    print("Please use gunicorn for development as well.")
    sys.exit(-1)
