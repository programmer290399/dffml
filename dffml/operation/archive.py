import uuid
from zipfile import ZipFile
from pathlib import Path
from typing import Union, Dict, Any, Optional

from ..df.base import op
from ..df.types import Definition


# definitions
DIRECTORY = Definition(name="directory", primitive="str")
ZIP_FILE = Definition(name="zip_file", primitive="str")


@op(
    inputs={"input_directory_path": DIRECTORY, "output_file_path": ZIP_FILE},
    outputs={},
)
async def make_zip_archive(
    input_directory_path: Union[Path, str],
    output_file_path: Optional[Union[Path, str]] = None,
):
    """
    Creates zip file of a directory.

    Parameters
    ----------
    input_directory_path : Union[Path, str]
        Path to directory to be archived
    output_file_path : Optional[Union[Path, str]], optional
        If set output file would be saved to this path otherwise 
        file is saved in the given directory, by default None
    """
    if output_file_path is None:
        file_name = str(uuid.uuid4()) + ".zip"
        output_file_path = Path(input_directory_path) / file_name
    with ZipFile(output_file_path, "w") as zip:
        for file in input_directory_path.rglob("*"):
            zip.write(file)


@op(
    inputs={"input_file_path": ZIP_FILE, "output_directory_path": DIRECTORY},
    outputs={},
)
async def extract_zip_archive(
    input_file_path: Union[Path, str],
    output_directory_path: Optional[Union[Path, str]] = None,
):
    """
    Extracts a given zip file.

    Parameters
    ----------
    input_file_path : Union[Path, str]
        Path to the zip file
    output_directory_path : Optional[Union[Path, str]], optional
        If set the file would be extracted to this path otherwise 
        the file would be extracted in the same directory as source, by default None
    """
    if output_directory_path is None:
        output_directory_path = Path(input_file_path).parent.absolute()
    with ZipFile(input_file_path, "r") as zip:
        zip.extractall(output_directory_path)
