import random
import string
from os import path

test_data_path = path.join(path.dirname(__file__), "test-data")


def random_string() -> str:
    return "".join(random.choice(string.ascii_lowercase) for _ in range(32))
