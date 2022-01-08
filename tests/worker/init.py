import sys

from celery import Celery
from kombu import Queue

# queues

py_queue = 'py_queue'

app = Celery(
    imports=['tasks'],
    task_serializer='json',
    accept_content=['json'],  # Ignore other content
    result_serializer='json',
    enable_utc=True,
    ignore_result=False,
    broker_url=sys.argv[1],
    result_backend=sys.argv[2],
    task_protocol=1)


def main():
    print('starting python worker')
    import_root_package()
    from tests.util.debugger import start_debugger
    start_debugger(9998)
    app.conf.task_queues = [
        Queue(name=app.conf.task_default_queue),
        Queue(name=py_queue)
    ]
    app.start(argv=['worker', '-P', 'threads', '-l', 'INFO'])


def import_root_package():
    import pathlib
    file = pathlib.Path(__file__).resolve()
    package_root_directory = file.parents[2]
    sys.path.append(str(package_root_directory))


if __name__ == '__main__':
    main()
