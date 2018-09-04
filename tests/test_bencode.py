import unittest
import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.path.pardir))
from bencoder import decode, encode


class MyTestCase(unittest.TestCase):
    def test_encoding_list(self):
        expected = b'li1eli2eli3eeee'
        result = encode([1, [2, [3]]])
        self.assertEqual(expected, result)

    def test_encoding_dictionary(self):
        expected = b'd2:doli1e1:ce3:how2:do3:youi42ee'
        result = encode({b'how': b'do', b'you': 42, b'do': [1, b'c']})
        self.assertEqual(expected, result)

    def test_decoding(self):
        expected = -42
        result = decode(b'i-42e')
        self.assertEquals(expected, result)

    def test_decoding_string(self):
        expected = b'ifyoureadthisyoureadthis'
        result = decode(b'24:ifyoureadthisyoureadthis')
        self.assertEquals(expected, result)

    def test_decoding_list(self):
        expected = [1, [2, [3]]]
        result = decode(b'li1eli2eli3eeee')
        self.assertEquals(expected, result)

    def test_decoding_dict(self):
        expected = {b'bar': b'spam', b'foo': 42, b'mess': [1, b'c']}
        result = decode(b'd3:bar4:spam3:fooi42e4:messli1e1:cee')
        self.assertEquals(expected, result)

if __name__ == '__main__':
    unittest.main()
