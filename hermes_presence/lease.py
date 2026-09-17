"""Native local-file lease: one gateway owns a data directory at a time."""
import os
from pathlib import Path
from .protocol import TemporalError


class GatewayLease:
    def __init__(self, data_dir):
        self.handle = (Path(data_dir) / '.gateway.lock').open('a+b')
        try:
            if os.name == 'nt':
                import msvcrt
                if self.handle.seek(0, os.SEEK_END) == 0:
                    self.handle.write(b'\0')
                    self.handle.flush()
                self.handle.seek(0)
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self.handle.close()
            raise TemporalError('another Presence gateway owns this data directory') from exc

    def close(self):
        self.handle.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
