from datetime import datetime
from uuid import UUID

# 128KB, a reasonable size for buffers
COPY_BUFFER_SIZE = 128 * 1024

MIN_UUID = UUID("0" * 32)

MAX_UUID = UUID("f" * 32)

FAR_FUTURE = datetime(9999, 1, 1)
