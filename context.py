import json
import logging
import os

from flask_mysql_util import MultiMySQL

from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)

__author__ = 'mshankar@slac.stanford.edu'

# Application context.
app = None

# Set up connections to the databases
logbook_db = MultiMySQL(prefix="LOGBOOK")
