import sys


def start_debugger(port: int):
    try:
        import pydevd_pycharm as pydevd
        pydevd.settrace(
            host='localhost',
            port=port,
            suspend=False,
            stdoutToServer=True,
            stderrToServer=True)
    except Exception as e:
        print("[Warning]: Could not enable debugger: " + str(e), file=sys.stderr)
        pass