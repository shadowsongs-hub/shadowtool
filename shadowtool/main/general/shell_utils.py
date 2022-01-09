import logging
import subprocess

import shadowtool.exceptions as exc

logger = logging.getLogger(__name__)


def bash_execute(statement: str, error_message: str = "", shell: bool = False) -> str:
    """
    Execute a bash script and give back the error to python to handle it
    :param statement: your bash code
    :param error_message: what you want to return when the script raises an error
    :param shell: True if you want to show the result in the shell, False if you want to give back the error to python
    :return: if the command succeed and gives back an output, return that output
    """
    stdout = None if shell else subprocess.PIPE
    p = subprocess.Popen(statement, shell=True, stdout=stdout, stderr=stdout)
    output, error = p.communicate()
    if error:
        logger.error(error.decode("utf-8"))
        if not error_message:
            error_message = f"\n\t Error generated : {error}\n\t Statement: {statement}"
        raise exc.BashCommandFailure(error_message)

    return output.decode("utf-8")


def run_commands(statement: str, error_message: str = "") -> None:
    p = subprocess.Popen(statement, shell=True, stderr=subprocess.PIPE)
    _, error = p.communicate()
    if error:
        raise Exception(error.decode("utf-8") + error_message)
