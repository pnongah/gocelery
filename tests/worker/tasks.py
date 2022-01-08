from init import app
from init import py_queue


@app.task(queue=py_queue)
def subtract(a, b):
    print('subtracting...')
    return a - b


@app.task
def throw_error():
    raise Exception("python error")
