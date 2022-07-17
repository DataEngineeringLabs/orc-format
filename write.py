import pyarrow as pa
import pyarrow.orc

data = {
    "a": [1.0, 2.0, None, 4.0, 5.0],
    "b": [True, False, None, True, False],
    "c": ["a", "cccccc", None, "ddd", "ee"],
    "d": ["a", "bb", None, "ccc", "ddd"],
    "e": ["ddd", "cc", None, "bb", "a"],
    "f": ["aaaaa", "bbbbb", None, "ccccc", "ddddd"],
}


def _write():
    import pyorc

    output = open("test.orc", "wb")
    writer = pyorc.Writer(
        output,
        "struct<a:float,b:boolean,c:string,d:string,e:string,f:string>",
        compression=pyorc.CompressionKind.NONE,
    )
    for x in range(5):
        row = (
            data["a"][x],
            data["b"][x],
            data["c"][x],
            data["d"][x],
            data["e"][x],
            data["f"][x],
        )
        writer.write(row)
    writer.close()

    example = open("test.orc", "rb")
    reader = pyorc.Reader(example)


_write()
