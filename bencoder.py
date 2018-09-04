#!/usr/bin/env python

import re
import string
import sys
import binascii


def encode(obj):
    if type(obj) == bytes:
        return str(len(obj)).encode() + b":" + obj
    elif type(obj) == dict:
        return b"d%se" % b"".join([encode(k) + encode(v) for k, v in sorted(obj.items())])
    elif type(obj) == list:
        return b"l%se" % b"".join(map(encode, obj))
    elif type(obj) == int:
        return b"i%ie" % obj
    raise ValueError("Invalid object (object must be a bytestring, an integer, a list or a "
                     "dictionary)")


def decode(input):
    def torrent_parsing(input):
        if type(input) != bytes:
            input = open(input, 'rb').read()
        if input.startswith(b"i"):
            match = re.match(b"i(-?\\d+)e", input)
            return int(match.group(1)), input[match.span()[1]:]
        elif input.startswith(b"l") or input.startswith(b"d"):
            list = []
            rest_elements = input[1:]
            while not rest_elements.startswith(b"e"):
                elem, rest_elements = torrent_parsing(rest_elements)
                list.append(elem)
            rest_elements = rest_elements[1:]
            if input.startswith(b"l"):
                return list, rest_elements
            else:
                return {i: j for i, j in zip(list[::2], list[1::2])}, rest_elements
        elif any(input.startswith(i.encode()) for i in string.digits):
            m = re.match(b"(\\d+):", input)
            length = int(m.group(1))
            start = m.span()[1]
            end = m.span()[1] + length
            return input[start:end], input[end:]
        else:
            raise ValueError("Wrong input")

    parse, rest = torrent_parsing(input)
    return parse

