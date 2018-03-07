from flask import Flask, current_app
import logging
import os
import sys
import json
from kafka import KafkaConsumer, TopicPartition, KafkaProducer
from threading import Thread
import eventlet
import requests
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler


from context import app, logbook_db

from pages import pages_blueprint

from dal.autorun import get_current_job_hashes, register_new_job_hash, get_new_experiments_after_id


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
    consumer = KafkaConsumer('experiment.register', bootstrap_servers=[os.environ.get("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")])

    while True:
        msg = next(consumer)

        if msg:
            logger.info("Message from Kafka %s", msg.value)
            info = json.loads(msg.value)
            experiment_name = info['experiment_name']

            default_auto_runs = load_default_auto_runs()
            auto_job_names = { x['hash'] for x in default_auto_runs }
            name2job = {x['hash'] : x for x in default_auto_runs }
            already_registered_jobs = { x['hash'] for x in get_current_job_hashes(experiment_name)}
            for unregistered_job in auto_job_names - already_registered_jobs:
                logger.info("Adding job hash %s for experiment %s ", unregistered_job, experiment_name)
                job_details = name2job[unregistered_job]
                register_new_job_hash(experiment_name, job_details)
            logger.info("Done processing experiment registration message for %s", experiment_name)


# Create thread for autorun
autorun_thread = Thread(target=insert_autorun_entries)
autorun_thread.start()

def check_and_publish_new_experiments():
    '''Poll the database for new experiments and publishes new experiment registrations.
    We store the last published experiment in a file that is read each timer tick'''
    try:
        last_exp_file = Path(os.environ["LAST_PUBLISHED_EXPERIMENT_FILE"])
        last_published_experiment_id = int(last_exp_file.read_text())
        new_experiments = get_new_experiments_after_id(last_published_experiment_id)
        if new_experiments:
            kafka_producer = KafkaProducer(bootstrap_servers=[os.environ.get("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")], value_serializer=lambda m: json.dumps(m).encode('ascii'))
            for new_experiment in new_experiments:
                try:
                    experiment_name = new_experiment["name"]
                    experiment_id = new_experiment["id"]
                    kafka_producer.send('experiment.register', { "experiment_name": experiment_name, "experiment_id": experiment_id })
                    logger.info("Published Kafka message for experiment %s name %s", experiment_id, experiment_name)
                    last_exp_file.write_text(str(experiment_id))
                    logger.info("Updated last published experiment file for experiment %s name %s", experiment_id, experiment_name)
                except Exception as e:
                    logger.exception("Exception publishing new experiment", e)
                    pass
            kafka_producer.close()
    except Exception:
        logger.exception("Exception processing new experiment registration")
        pass

scheduler = BackgroundScheduler()
scheduler.add_job(check_and_publish_new_experiments, 'interval', seconds=60, id="check_and_publish_new_experiments")
scheduler.start()

if __name__ == '__main__':
    print("Please use gunicorn for development as well.")
    sys.exit(-1)
