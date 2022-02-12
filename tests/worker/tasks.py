import threading
from time import sleep
from typing import Any, Callable

from celery import Task
from celery.concurrency import gevent

from init import app
from init import py_queue


@app.task(queue=py_queue)
def subtract(a, b):
    print('subtracting...')
    return a - b


@app.task
def throw_error():
    raise Exception("python error")


@app.task(bind=True)
def progress(task: Task):
    threads: list[threading.Thread] = []

    # asynchronous version
    def async_progress_callback(meta: Any):
        def update(task_id: str):
            sleep(0.5)  # simulate delay
            task.update_state(task_id=task_id, state='PROGRESS', meta=meta)

        th = threading.Thread(target=update, args=[task.request.id])
        th.start()
        threads.append(th)

    # synchronous version
    def sync_progress_callback(meta: Any):
        sleep(0.5)  # simulate delay
        task.update_state(task_id=task.request.id, state='PROGRESS', meta=meta)

    progress_task(async_progress_callback)

    for t in threads:
        t.join()

    return "progress complete!"


def progress_task(progress_callback: Callable[[Any], None]):
    for i in range(101):
        sleep(0.1)
        print('iteration: {}'.format(i))
        progress_callback({'current': i, 'total': 100})
