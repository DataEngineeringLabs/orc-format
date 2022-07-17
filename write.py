import pyorc

data = {
    "a": [1.0, 2.0, None, 4.0, 5.0],
    "b": [True, False, None, True, False],
    "str_direct": ["a", "cccccc", None, "ddd", "ee"],
    "d": ["a", "bb", None, "ccc", "ddd"],
    "e": ["ddd", "cc", None, "bb", "a"],
    "f": ["aaaaa", "bbbbb", None, "ccccc", "ddddd"],
    "int_short_repeated": [5, 5, None, 5, 5],
    "int_neg_short_repeated": [-5, -5, None, -5, -5],
    "int_delta": [1, 2, None, 4, 5],
    "int_neg_delta": [5, 4, None, 2, 1],
    "int_direct": [1, 6, None, 3, 2],
    "int_neg_direct": [-1, -6, None, -3, -2],
}


def _write():
    output = open("test.orc", "wb")
    writer = pyorc.Writer(
        output,
        "struct<a:float,b:boolean,str_direct:string,d:string,e:string,f:string,int_short_repeated:int,int_neg_short_repeated:int,int_delta:int,int_neg_delta:int,int_direct:int,int_neg_direct:int>",
        compression=pyorc.CompressionKind.NONE,
    )
    for x in range(5):
        row = (
            data["a"][x],
            data["b"][x],
            data["str_direct"][x],
            data["d"][x],
            data["e"][x],
            data["f"][x],
            data["int_short_repeated"][x],
            data["int_neg_short_repeated"][x],
            data["int_delta"][x],
            data["int_neg_delta"][x],
            data["int_direct"][x],
            data["int_neg_direct"][x],
        )
        writer.write(row)
    writer.close()

    example = open("test.orc", "rb")
    reader = pyorc.Reader(example)
    print(list(reader))


_write()
