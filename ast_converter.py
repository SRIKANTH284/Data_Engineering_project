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

# Step 4: Define transformation logic for read/write
class BatchToStreamTransformer(ast.NodeTransformer):
    def visit_Attribute(self, node):
        if node.attr == "read":
            node.attr = "readStream"
        elif node.attr == "write":
            node.attr = "writeStream"
        return self.generic_visit(node)

# Step 5: Apply transformation
transformer = BatchToStreamTransformer()
transformed_tree = transformer.visit(tree)

# Step 6: Convert the transformed AST back to Python source code
stream_code = astor.to_source(transformed_tree)

# Step 7: Save to stream.py instead of printing
output_file = "stream.py"
with open(output_file, "w") as f_out:
    f_out.write(stream_code)

print(f" Streaming code has been saved to '{output_file}'")
