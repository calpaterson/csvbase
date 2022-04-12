from typing import Union, Mapping, Optional
import io
import codecs

import werkzeug.datastructures

from .value_objs import Column, PythonType

# FIXME: the two type synonyms below should be proper Protocols but I haven't
# the time or inclination at the moment

UserSubmittedCSVData = Union[codecs.StreamReader, io.StringIO]

UserSubmittedBytes = Union[werkzeug.datastructures.FileStorage, io.BytesIO]

Row = Mapping[Column, Optional[PythonType]]
