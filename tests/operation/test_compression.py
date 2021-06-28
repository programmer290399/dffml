from pathlib import PosixPath
from unittest.mock import patch, mock_open, call

from .test_archive import create_dataflow

from dffml import run
from dffml.df.base import OperationException
from dffml.util.asynctestcase import AsyncTestCase
from dffml.operation.compression import (
    compress,
    de_compress,
    UnsupportedCompressionFormatError,
)


class TestCompressionOperations(AsyncTestCase):
    uncomressed_file_pth = "test/path/to/uncompressed_file.ext"
    compressed_file_pth = (
        lambda self, file_format: f"test/path/to/compressed_file{file_format}"
    )

    def get_creation_mock_calls(self, file_format):
        return [
            call(),
            call(),
            call()(self.uncomressed_file_pth, "rb"),
            call()().__enter__(),
            call()(self.compressed_file_pth(file_format), "wb"),
            call()().__enter__(),
            call()().__exit__(None, None, None),
            call()().__exit__(None, None, None),
        ]

    def get_inflation_mock_calls(self, file_format):
        return [
            call(),
            call(),
            call()(PosixPath(self.compressed_file_pth(file_format)), "rb"),
            call()().__enter__(),
            call()(self.uncomressed_file_pth, "wb"),
            call()().__enter__(),
            call()().__exit__(None, None, None),
            call()().__exit__(None, None, None),
        ]

    async def test_create_gz(self):
        dataflow = create_dataflow(
            compress,
            {
                "input_file_path": self.uncomressed_file_pth,
                "output_file_path": self.compressed_file_pth(".gz"),
                "file_format": ".gz",
            },
        )
        m_open = mock_open()
        with patch("builtins.open", m_open()), patch(
            "gzip.open", m_open()
        ), patch("shutil.copyfileobj"):
            async for _, _ in run(dataflow):
                m_open.assert_has_calls(self.get_creation_mock_calls(".gz"))

    async def test_create_bz2(self):
        dataflow = create_dataflow(
            compress,
            {
                "input_file_path": self.uncomressed_file_pth,
                "output_file_path": self.compressed_file_pth(".bz2"),
                "file_format": ".bz2",
            },
        )
        m_open = mock_open()
        with patch("builtins.open", m_open()), patch(
            "bz2.open", m_open()
        ), patch("shutil.copyfileobj"):
            async for _, _ in run(dataflow):
                m_open.assert_has_calls(self.get_creation_mock_calls(".bz2"))

    async def test_create_xz(self):
        dataflow = create_dataflow(
            compress,
            {
                "input_file_path": self.uncomressed_file_pth,
                "output_file_path": self.compressed_file_pth(".xz"),
                "file_format": ".xz",
            },
        )
        m_open = mock_open()
        with patch("builtins.open", m_open()), patch(
            "lzma.open", m_open()
        ), patch("shutil.copyfileobj"):
            async for _, _ in run(dataflow):
                m_open.assert_has_calls(self.get_creation_mock_calls(".xz"))

    async def test_inflate_gz(self):
        dataflow = create_dataflow(
            de_compress,
            {
                "input_file_path": self.compressed_file_pth(".gz"),
                "output_file_path": self.uncomressed_file_pth,
            },
        )
        m_open = mock_open()
        with patch("builtins.open", m_open()), patch(
            "gzip.open", m_open()
        ), patch("shutil.copyfileobj"):
            async for _, _ in run(dataflow):
                m_open.assert_has_calls(self.get_inflation_mock_calls(".gz"))

    async def test_inflate_bz2(self):
        dataflow = create_dataflow(
            de_compress,
            {
                "input_file_path": self.compressed_file_pth(".bz2"),
                "output_file_path": self.uncomressed_file_pth,
            },
        )
        m_open = mock_open()
        with patch("builtins.open", m_open()), patch(
            "bz2.open", m_open()
        ), patch("shutil.copyfileobj"):
            async for _, _ in run(dataflow):
                m_open.assert_has_calls(self.get_inflation_mock_calls(".bz2"))

    async def test_inflate_xz(self):
        dataflow = create_dataflow(
            de_compress,
            {
                "input_file_path": self.compressed_file_pth(".xz"),
                "output_file_path": self.uncomressed_file_pth,
            },
        )
        m_open = mock_open()
        with patch("builtins.open", m_open()), patch(
            "lzma.open", m_open()
        ), patch("shutil.copyfileobj"):
            async for _, _ in run(dataflow):
                m_open.assert_has_calls(self.get_inflation_mock_calls(".xz"))

    async def test_compress_raises_UnsupportedCompressionFormatError(self):
        dataflow = create_dataflow(
            compress,
            {
                "input_file_path": self.uncomressed_file_pth,
                "output_file_path": self.compressed_file_pth(".xyz"),
                "file_format": ".xyz",
            },
        )
        with self.assertRaises(OperationException):
            with self.assertRaises(UnsupportedCompressionFormatError):
                async for _, _ in run(dataflow):
                    pass

    async def test_de_compress_raises_UnsupportedCompressionFormatError(self):
        dataflow = create_dataflow(
            de_compress,
            {
                "input_file_path": self.compressed_file_pth(".xyz"),
                "output_file_path": self.uncomressed_file_pth,
            },
        )
        with self.assertRaises(OperationException):
            with self.assertRaises(UnsupportedCompressionFormatError):
                async for _, _ in run(dataflow):
                    pass
