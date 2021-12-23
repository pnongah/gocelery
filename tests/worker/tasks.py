from main import app


@app.task
def subtract(a, b):
    print('subtracting...')
    return a - b


@app.task
def throw_error():
    raise Exception("python error")
