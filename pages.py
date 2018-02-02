import os
import json
import logging
import pkg_resources

import context

from flask import Blueprint, render_template, send_file, abort

pages_blueprint = Blueprint('pages_api', __name__)

logger = logging.getLogger(__name__)

@pages_blueprint.route("/")
def index():
    return render_template("index.html")
