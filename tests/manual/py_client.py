# Intended for making manual celery calls using python.

from celery import Celery

# adjust these based on running containers
broker_url = 'amqp://admin:root@localhost:5672'
backend_url = 'redis://localhost:6379'


def call_go_worker():
    celery = Celery(task_serializer='json',
                    accept_content=['json'],  # Ignore other content
                    result_serializer='json',
                    enable_utc=True,
                    ignore_result=False,
                    broker_url=broker_url + '//go-worker',
                    result_backend=backend_url + '/0',
                    task_protocol=1)
    print('calling add from python')
    result = celery.send_task('add', (1, 2))
    print('Result of add: {0}'.format(result.get()))


def call_py_worker():
    celery = Celery(task_serializer='json',
                    accept_content=['json'],  # Ignore other content
                    result_serializer='json',
                    enable_utc=True,
                    ignore_result=False,
                    broker_url=broker_url + '//py-worker',
                    result_backend=backend_url + '/1',
                    task_protocol=1)
    print('calling subtract from python')
    result = celery.send_task('tasks.subtract', (3, 2))
    print('Result of tasks.subtract: {0}'.format(result.get()))


if __name__ == '__main__':
    call_py_worker()
    call_go_worker()
