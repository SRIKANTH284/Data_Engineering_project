import ast
import astor
import argparse

# Step 1: Parse command-line argument to get input file
parser = argparse.ArgumentParser(description="Transform batch Spark code to streaming using AST.")
parser.add_argument("input_file", help="Path to the input Python file containing batch Spark code")
args = parser.parse_args()

# Step 2: Read the input batch code
with open(args.input_file, "r") as file:
    external_code = file.read()

# Step 3: Parse it into an AST
tree = ast.parse(external_code)

# Step 4: Define transformation logic
class BatchToStreamTransformer(ast.NodeTransformer):
    def visit_Attribute(self, node):
        if node.attr == "read":
            node.attr = "readStream"
        elif node.attr == "write":
            node.attr = "writeStream"
        return self.generic_visit(node)

    def visit_Call(self, node):
        if isinstance(node.func, ast.Attribute):
            # Modify readStream
            if node.func.attr == 'readStream':
                # Insert .option('header', True).schema(schema)
                node = ast.Call(
                    func=ast.Attribute(value=node, attr='option'),
                    args=[ast.Str(s='header'), ast.Constant(value=True)],
                    keywords=[]
                )
                node = ast.Call(
                    func=ast.Attribute(value=node, attr='schema'),
                    args=[ast.Name(id='schema', ctx=ast.Load())],
                    keywords=[]
                )
                # Now use wildcard to read all CSVs in stream_data/
                node = ast.Call(
                    func=ast.Attribute(value=node, attr='csv'),
                    args=[ast.Str(s='stream_data/*.csv')],  # Wildcard to load all CSVs
                    keywords=[]
                )
                return node

            # Modify writeStream
            if node.func.attr == 'writeStream':
                # Chain .outputMode("append").format("csv").option("path", "stream_output/").option("checkpointLocation", "chkpt").start()
                node = ast.Call(
                    func=ast.Attribute(value=node, attr='outputMode'),
                    args=[ast.Str(s='append')],
                    keywords=[]
                )
                node = ast.Call(
                    func=ast.Attribute(value=node, attr='format'),
                    args=[ast.Str(s='csv')],
                    keywords=[]
                )
                node = ast.Call(
                    func=ast.Attribute(value=node, attr='option'),
                    args=[ast.Str(s='path'), ast.Str(s='stream_output/')],
                    keywords=[]
                )
                node = ast.Call(
                    func=ast.Attribute(value=node, attr='option'),
                    args=[ast.Str(s='checkpointLocation'), ast.Str(s='chkpt')],
                    keywords=[]
                )
                node = ast.Call(
                    func=ast.Attribute(value=node, attr='start'),
                    args=[],
                    keywords=[]
                )
                return node

        return self.generic_visit(node)

# Step 5: Apply transformation
transformer = BatchToStreamTransformer()
transformed_tree = transformer.visit(tree)

# Step 6: Convert the transformed AST back to Python source code
stream_code = astor.to_source(transformed_tree)

# Step 7: Add the schema definition manually at the top
schema_code = """
from pyspark.sql.types import StructType, StructField, StringType, FloatType

schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("category", StringType(), True),
    StructField("value", FloatType(), True)
])
"""

final_code = schema_code + "\n" + stream_code

# Step 8: Save to stream.py
output_file = "stream.py"
with open(output_file, "w") as f_out:
    f_out.write(final_code)

print(f"✅ Streaming code has been saved to '{output_file}' and is ready to run!")
