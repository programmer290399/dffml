import bz2
import gzip
import lzma
import shutil
from pathlib import Path

from ..df.base import op
from ..df.types import Definition

# definitions
INPUT_FILE_PATH = Definition(name="input_file_path", primitive="str")
OUTPUT_FILE_PATH = Definition(name="output_file_path", primitive="str")
FORMAT = Definition(name="format", primitive="str")

SUPPORTED_COMPRESSION_FORMATS = {".gz": gzip, ".bz2": bz2, ".xz": lzma}


class UnsupportedArchiveFormatError(Exception):
    def __init__(self, format):
        super().__init__()
        self.format = format

    def __str__(self):
        return f"{self.format} format is not currently supported."


def get_compression_class(format):
    compression_cls = SUPPORTED_COMPRESSION_FORMATS.get(format, None)
    if compression_cls is None:
        raise UnsupportedArchiveFormatError(format)
    return compression_cls


@op(
    inputs={
        "input_file_path": INPUT_FILE_PATH,
        "output_file_path": OUTPUT_FILE_PATH,
        "file_format": FORMAT,
    },
    outputs={},
)
async def compress(
    input_file_path: str, output_file_path: str, file_format: str
):
    compression_cls = get_compression_class(file_format)
    with open(input_file_path, "rb") as f_in:
        with compression_cls.open(output_file_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)


@op(
    inputs={
        "input_file_path": INPUT_FILE_PATH,
        "output_file_path": OUTPUT_FILE_PATH,
    },
    outputs={},
)
async def de_compress(input_file_path: str, output_file_path: str):
    input_file_path = Path(input_file_path)
    file_format = input_file_path.suffix
    compression_cls = get_compression_class(file_format)
    with compression_cls.open(input_file_path, "rb") as f_in:
        with open(output_file_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
