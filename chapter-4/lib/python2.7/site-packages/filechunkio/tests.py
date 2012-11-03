import io
import os
import tempfile
import unittest

from filechunkio import FileChunkIO
from filechunkio import SEEK_CUR
from filechunkio import SEEK_END


class FileChunkIOTest(unittest.TestCase):

    def setUp(self):
        self.tf = tempfile.mkstemp()[1]

        with open(self.tf, 'w') as f:
            f.write('123456789%s' % os.linesep)
            f.write('234567891%s' % os.linesep)
            f.write('345678912%s' % os.linesep)
            f.write('456789123%s' % os.linesep)
            f.write('56789')

    def tearDown(self):
        if os.path.exists(self.tf):
            os.remove(self.tf)

    def test_open_write_raises_valueerror(self):
        self.assertRaises(ValueError, FileChunkIO, self.tf, 'w')

    def test_init_autosets_bytes(self):
        with FileChunkIO(self.tf) as c:
            self.assertEquals(c.bytes, 45)

    def test_init_autosets_bytes_and_respects_offset(self):
        with FileChunkIO(self.tf, offset=1) as c:
            self.assertEquals(c.bytes, 44)

    def test_init_seeks_to_offset(self):
        with FileChunkIO(self.tf, offset=1) as c:
            self.assertEquals(c.tell(), 0)
            self.assertEquals(c.read(1), '2')

    def test_seek_respects_offset(self):
        with FileChunkIO(self.tf, offset=1) as c:
            c.seek(1)
            self.assertEquals(c.read(1), '3')

    def test_seek_cur(self):
        with FileChunkIO(self.tf, offset=20, bytes=10) as c:
            c.seek(5)
            c.seek(-3, SEEK_CUR)
            self.assertEquals(c.tell(), 2)

    def test_seek_end(self):
        with FileChunkIO(self.tf, offset=10, bytes=10) as c:
            c.seek(-5, SEEK_END)
            self.assertEquals(c.read(3), '789')

    def test_tell_respects_offset(self):
        with FileChunkIO(self.tf, offset=1) as c:
            self.assertEquals(c.tell(), 0)
            self.assertEquals(c.read(1), '2')

    def test_read_with_minus_one_calls_readall(self):
        with FileChunkIO(self.tf) as c:
            def mocked_readall(*args, **kwargs):
                self._readall_called = True
            c.readall = mocked_readall

            c.read(-1)
            self.assertTrue(self._readall_called)

    def test_read_respects_offset_and_bytes(self):
        with FileChunkIO(self.tf, offset=1, bytes=3) as c:
            self.assertEquals(c.read(), '234')

    def test_readinto(self):
        with FileChunkIO(self.tf, offset=1, bytes=2) as c:
            n = 3
            b = bytearray(n.__index__())
            c.readinto(b)
            self.assertEquals(b, b'23\x00')

    def test_readline(self):
        with FileChunkIO(self.tf, offset=1, bytes=20) as c:
            lines = []
            while True:
                line = c.readline()
                if not line:
                    break
                lines.append(line)
            self.assertEquals(lines, ['23456789\n', '234567891\n', '3'])

    def test_readlines(self):
        with FileChunkIO(self.tf, offset=1, bytes=15) as c:
            self.assertEquals(c.readlines(), ['23456789\n', '234567'])

    def test_next(self):
        with FileChunkIO(self.tf, offset=1, bytes=20) as c:
            lines = []
            while True:
                try:
                    lines.append(c.next())
                except StopIteration:
                    break
            self.assertEquals(lines, ['23456789\n', '234567891\n', '3'])


if __name__ == '__main__':
    unittest.main()
