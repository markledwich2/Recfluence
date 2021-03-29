import dataclasses
import io
import json
from typing import Any
from datetime import datetime, timezone


class JsonlEncoder(json.JSONEncoder):
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        if isinstance(o, datetime):
            # create a standard json formatted datetime
            return o.strftime('%Y-%m-%dT%H:%M:%S.%fZ') if o.tzinfo is None or o.tzinfo == timezone.utc else o.isoformat()
        return super().default(o)

    def encode(self, obj, *args, **kwargs):
        lines = []
        for each in obj:
            line = super(JsonlEncoder, self).encode(each, *args, **kwargs)
            lines.append(line)
        return '\n'.join(lines)


def write_jsonl(obj: Any, fp: io.IOBase, cls=None, **kwargs):
    text = to_jsonl(obj, cls, **kwargs)
    if(isinstance(fp, (io.RawIOBase, io.BufferedIOBase))):
        fp.write(text.encode())
    else:
        fp.write(text)


def to_jsonl(obj: Any, cls=None, **kwargs):
    if cls is None:
        cls = JsonlEncoder
    return cls(**kwargs).encode(obj)
