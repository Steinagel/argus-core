#!/usr/bin/env python
import tika
tika.initVM()
from tika import parser
parsed = parser.from_file('testfiles/test.html')
print(parsed["metadata"])
print(parsed["content"])
