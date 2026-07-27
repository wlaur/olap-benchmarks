import subprocess


def run_shell(command: str) -> int:
    return subprocess.run(command, shell=True, check=False).returncode
