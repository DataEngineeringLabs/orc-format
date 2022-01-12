import pyorc
import pyarrow
import pyarrow.orc

with open("./test.orc", "wb") as f:
    writer = pyorc.Writer(f, "struct<col0:int,col1:string>")
    writer.write((0, "Test 0"))
    #writer.write((1, "Test 1"))
    #writer.write((2, "Test 2"))
    writer.close()



table = pyarrow.Table.from_arrays([pyarrow.array([1, 2, 3])], schema=pyarrow.schema([("a", pyarrow.int32())]))

pyarrow.orc.write_table(table, "test.orc")


with open("./test.orc", "br") as f:
    writer = pyorc.Reader(f)
    a = writer.read()
    print(a)
