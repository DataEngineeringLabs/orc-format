import pyarrow as pa
import pyarrow.orc

table = pa.Table.from_arrays(
    [pa.array([1.0, None, 3.0])], schema=pa.schema([("a", pa.float32())])
)

pyarrow.orc.write_table(table, "test.orc")


def _write():
    import pyorc

    output = open("test.orc", "wb")
    writer = pyorc.Writer(output, "struct<a:float>", compression = pyorc.CompressionKind.NONE)
    for x in range(5):
        writer.write((1.0,))
        writer.write((None,))
    writer.close()

    example = open("test.orc", "rb")
    reader = pyorc.Reader(example)
    print(str(reader.schema))

    stripe2 = reader.read_stripe(0)
    print(stripe2.bytes_offset)
    print(stripe2.bytes_length)

    assert list(reader) == [(1.0,), (None,)]*5
