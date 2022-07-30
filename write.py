import random

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
    "bigint_direct": [1, 6, None, 3, 2],
    "bigint_neg_direct": [-1, -6, None, -3, -2],
    "bigint_other": [5, -5, 1, 5, 5],
}

def infer_schema(data):
    schema = "struct<"
    for key, value in data.items():
        dt = type(value[0])
        if dt == float:
            dt = "float"
        elif dt == int:
            dt = "int"
        elif dt == bool:
            dt = "boolean"
        elif dt == str:
            dt = "string"
        else:
            raise NotImplementedError
        if key.startswith("double"):
            dt = "double"
        if key.startswith("bigint"):
            dt = "bigint"
        schema += key + ":" + dt + ","

    schema = schema[:-1] + ">"
    return schema



def _write(
    schema: str,
    data,
    file_name: str,
    compression=pyorc.CompressionKind.NONE,
    dict_key_size_threshold=0.0,
):
    output = open(file_name, "wb")
    writer = pyorc.Writer(
        output,
        schema,
        dict_key_size_threshold=dict_key_size_threshold,
        # use a small number to ensure that compression crosses value boundaries
        compression_block_size=32,
        compression=compression,
    )
    num_rows = len(list(data.values())[0])
    for x in range(num_rows):
        row = tuple(values[x] for values in data.values())
        writer.write(row)
    writer.close()

    with open(file_name, "rb") as f:
        reader = pyorc.Reader(f)
        list(reader)


_write(
    infer_schema(data),
    data,
    "test.orc",
)

data_boolean = {
    "long": [True] * 32,
}

_write("struct<long:boolean>", data_boolean, "long_bool.orc")

_write("struct<long:boolean>", data_boolean, "long_bool_gzip.orc", pyorc.CompressionKind.ZLIB)

data_dict = {
    "dict": ["abcd", "efgh"] * 32,
}

_write("struct<dict:string>", data_dict, "string_long.orc")

data_dict = {
    "dict": ["abc", "efgh"] * 32,
}

_write("struct<dict:string>", data_dict, "string_dict.orc", dict_key_size_threshold=0.1)

_write("struct<dict:string>", data_dict, "string_dict_gzip.orc", pyorc.CompressionKind.ZLIB)

data_dict = {
    "dict": ["abcd", "efgh"] * (10**4 // 2),
}

_write("struct<dict:string>", data_dict, "string_long_long.orc")
_write("struct<dict:string>", data_dict, "string_long_long_gzip.orc", pyorc.CompressionKind.ZLIB)

long_f32 = {
    "dict": [random.uniform(0, 1) for _ in range(10**6)],
}

_write("struct<dict:float>", long_f32, "f32_long_long_gzip.orc", pyorc.CompressionKind.ZLIB)
