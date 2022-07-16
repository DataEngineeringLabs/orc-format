import pyarrow as pa
import pyarrow.orc

data = {
    "a": [1.0, 2.0, 3.0, 4.0, 5.0],
    "b": [True, False, None, True, False],
    "c": ["a", "cccccc", None, "ddd", "ee"],
}


def _write():
    import pyorc

    output = open("test.orc", "wb")
    writer = pyorc.Writer(
        output, "struct<a:float,b:boolean,c:string>", compression=pyorc.CompressionKind.NONE
    )
    for x in range(5):
        writer.write((data["a"][x], data["b"][x], data["c"][x]))
    writer.close()

    example = open("test.orc", "rb")
    reader = pyorc.Reader(example)
    print(str(reader.schema))

    stripe2 = reader.read_stripe(0)
    print(stripe2.bytes_offset)
    print(stripe2.bytes_length)


_write()
