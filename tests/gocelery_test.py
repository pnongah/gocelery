import sys
import time
import unittest
from unittest import TestSuite

from celery import Celery
from celery.result import AsyncResult

celery_client = None


class CeleryTest(unittest.TestCase):

    def __init__(self, test_case: str):
        super().__init__(test_case)
        self.celery_client = celery_client

    def test_pyworker_subtract(self):
        print('calling subtract from python')
        result: AsyncResult = self.celery_client.send_task('tasks.subtract', args=(3, 2), exchange='celery',
                                                           queue='py_queue')
        print('received async result for subtract with ID {}'.format(result.id))
        val = result.get(timeout=5)
        self.assertEqual(1, val)

    def test_goworker_add(self):
        print('calling add from python')
        result: AsyncResult = self.celery_client.send_task('add', args=(1, 2), exchange='celery', queue='go_queue')
        print('received async result for add with ID {}'.format(result.id))
        val = result.get(timeout=5)  # flaky... backend result is available but the celery code keeps timing out (sometimes works though)
        # val = result.maybe_throw()
        self.assertEqual(3, val)


def main():
    print('starting python tests with args {}'.format(sys.argv))
    start_debugger()
    broker_url = sys.argv[1]
    backend_url = sys.argv[2]
    test_cases = sys.argv[3:]
    global celery_client
    celery_client = Celery(task_serializer='json',
                           accept_content=['json'],  # Ignore other content
                           result_serializer='json',
                           enable_utc=True,
                           ignore_result=False,
                           broker_url=broker_url,
                           result_backend=backend_url,
                           task_protocol=1)
    run_tests(test_cases)


def run_tests(test_cases: [str]):
    suite: TestSuite
    if len(test_cases) > 0:
        suite = unittest.TestSuite()
        for testCase in test_cases:
            suite.addTest(CeleryTest(testCase))
    else:
        suite = unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(CeleryTest))
    runner = unittest.TextTestRunner()
    result = runner.run(suite)
    if result.wasSuccessful():
        exit(0)
    else:
        exit(1)


def start_debugger():
    try:
        import pydevd_pycharm as pydevd
        pydevd.settrace(
            host='localhost',
            port=9999,
            stdoutToServer=True,
            stderrToServer=True)
    except Exception as e:
        print("[Warning]: Could not enable debugger: " + str(e), file=sys.stderr)
        pass


if __name__ == '__main__':
    main()
