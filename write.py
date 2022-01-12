import pyarrow as pa
import pyarrow.orc

table = pa.Table.from_arrays(
    [pa.array([1, 2, 3])], schema=pa.schema([("a", pa.int32())])
)

pyarrow.orc.write_table(table, "test.orc")
