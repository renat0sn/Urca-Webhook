from flask import Flask
import os

app = Flask(__name__)

@app.route("/")
def index():
    return "Hello World! Heroku"

if __name__ == '__main__':
    app.run()