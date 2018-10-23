import os

from flask import Flask
from flask_sqlalchemy import SQLAlchemy

pwd = os.path.dirname(os.path.abspath(__file__))

app = Flask('optionchan')
app.config.from_object('webapp.config')
db = SQLAlchemy(app)

import webapp.views
