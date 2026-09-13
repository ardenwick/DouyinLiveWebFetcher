

import sys


def read_file(file_path: str) -> str:
    with open(file_path, 'r', encoding="utf8") as f:
        return f.read()


def write_file(file_path: str, text: str, append=False) -> None:
    with open(file_path, 'a' if append else 'w', encoding='utf8') as f:
        f.write(text)


def write_stderr(text: str):
    sys.stderr.write(text)
    sys.stderr.flush()
