import pyarrow as pa
import pyarrow.orc

table = pa.Table.from_arrays(
    [pa.array([1, 2, 3])], schema=pa.schema([("a", pa.int32())])
)

pyarrow.orc.write_table(table, "test.orc")

import pyorc

#output = open("test.orc", "wb")
#writer = pyorc.Writer(output, "struct<a:int>")
#writer.write((0,))
#writer.write((1,))
#writer.write((3,))
#writer.close()

example = open("test.orc", "rb")
reader = pyorc.Reader(example)
print(str(reader.schema))

stripe2 = reader.read_stripe(0)
print(stripe2.bytes_offset)
print(stripe2.bytes_length)
