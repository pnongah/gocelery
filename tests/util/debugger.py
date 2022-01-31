import os
import sys
import threading
import time


def start_debugger(port: int):
    host = os.getenv("PYTHON_DEBUG_HOST", default="localhost")
    port = int(os.getenv("PYTHON_DEBUG_PORT", default=str(port)))
    t = threading.Thread(target=__start_debugger, args=(host, port))
    t.daemon = True
    t.start()


def __start_debugger(host: str, port: int):
    print("Remote debugging configured for host={}, port={}".format(host, port))
    stderr_original = sys.stderr
    while True:
        try:
            import pydevd_pycharm as pydevd
            sys.stderr = open(os.devnull, "w")  # needed otherwise pycharm might spam the logs.
            # unfortunately not thread safe as this is global
            pydevd.settrace(host=host, port=port, suspend=False, stdoutToServer=True, stderrToServer=True)
            print("Remote debugging enabled")
            return
        except ConnectionRefusedError or TimeoutError:
            sys.stderr = stderr_original
            time.sleep(1)
            pass
        except Exception as e:
            sys.stderr = stderr_original
            raise Exception("Unknown error while trying to connect to a debugger") from e
