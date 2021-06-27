from unittest.mock import patch, mock_open

from dffml import run
from dffml.df.types import DataFlow, Input
from dffml.util.asynctestcase import AsyncTestCase
from dffml.operation.archive import make_zip_archive, extract_zip_archive


class TestZipOperations(AsyncTestCase):
    test_file_pth = "test/path/to/zip_file.zip"
    test_dir_pth = "test/path/to/directory"

    def _create_dataflow(self, operation, seed):
        dataflow = DataFlow(
            operations={operation.op.name: operation},
            seed=seed,
            implementations={operation.op.name: operation.imp},
        )
        return dataflow

    async def test_make_zip_op(self):
        dataflow = self._create_dataflow(
            make_zip_archive,
            {
                Input(
                    value=self.test_dir_pth,
                    definition=make_zip_archive.op.inputs[
                        "input_directory_path"
                    ],
                ),
                Input(
                    value=self.test_file_pth,
                    definition=make_zip_archive.op.inputs["output_file_path"],
                ),
            },
        )
        m_open = mock_open()
        with patch("io.open", m_open), patch(
            "zipfile.ZipFile._write_end_record"
        ):
            async for _, _ in run(dataflow):
                m_open.assert_called_once_with(self.test_file_pth, "w+b")

    async def test_extract_zip_op(self):
        dataflow = self._create_dataflow(
            extract_zip_archive,
            {
                Input(
                    value=self.test_file_pth,
                    definition=extract_zip_archive.op.inputs[
                        "input_file_path"
                    ],
                ),
                Input(
                    value=self.test_dir_pth,
                    definition=extract_zip_archive.op.inputs[
                        "output_directory_path"
                    ],
                ),
            },
        )
        m_open = mock_open()
        with patch("io.open", m_open), patch("zipfile._EndRecData"), patch(
            "zipfile.ZipFile._RealGetContents"
        ):
            async for _, _ in run(dataflow):
                m_open.assert_called_once_with(self.test_file_pth, "rb")
