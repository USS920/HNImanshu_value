from flask import Flask
import os

app = Flask(__name__)

@app.route('/')
def run_script():
    os.system('/bin/bash auto.sh')
    return "Script executed!"
