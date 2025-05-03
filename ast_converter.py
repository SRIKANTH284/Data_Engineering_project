import ast
import astor
import argparse

class BatchToStreamTransformer(ast.NodeTransformer):
    def __init__(self):
        super().__init__()
        self.df_name = None

    def visit_Assign(self, node):
        if isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Attribute):
            if node.value.func.attr == "csv":
                self.df_name = node.targets[0].id

                # Use spark.readStream instead of spark.read
                read_stream_base = ast.Attribute(
                    value=ast.Name(id='spark', ctx=ast.Load()),
                    attr='readStream',
                    ctx=ast.Load()
                )

                read_node = ast.Call(
                    func=ast.Attribute(value=read_stream_base, attr='option', ctx=ast.Load()),
                    args=[ast.Str(s='header'), ast.NameConstant(value=True)],
                    keywords=[]
                )
                read_node = ast.Call(
                    func=ast.Attribute(value=read_node, attr='option', ctx=ast.Load()),
                    args=[ast.Str(s='maxFilesPerTrigger'), ast.Str(s='1')],
                    keywords=[]
                )
                read_node = ast.Call(
                    func=ast.Attribute(value=read_node, attr='schema', ctx=ast.Load()),
                    args=[ast.Name(id='schema', ctx=ast.Load())],
                    keywords=[]
                )
                read_node = ast.Call(
                    func=ast.Attribute(value=read_node, attr='csv', ctx=ast.Load()),
                    args=[ast.Str(s='stream_input/')],
                    keywords=[]
                )
                node.value = read_node
                
        return node

    def visit_Expr(self, node):
        # Detect and replace df.write.csv(...) as an expression
        if isinstance(node.value, ast.Call):
            # Check if this is a write operation
            if (isinstance(node.value.func, ast.Attribute) and 
                node.value.func.attr == 'csv' and 
                isinstance(node.value.func.value, ast.Attribute) and 
                node.value.func.value.attr == 'write'):
                
                # Get the dataframe name
                df_name = node.value.func.value.value.id
                
                # Create the streaming write operation
                write_stream_base = ast.Attribute(
                    value=ast.Name(id=df_name, ctx=ast.Load()),
                    attr='writeStream',
                    ctx=ast.Load()
                )

                # Build the streaming query
                query_node = ast.Call(
                    func=ast.Attribute(value=write_stream_base, attr='outputMode', ctx=ast.Load()),
                    args=[ast.Str(s='complete')], keywords=[]
                )
                query_node = ast.Call(
                    func=ast.Attribute(value=query_node, attr='format', ctx=ast.Load()),
                    args=[ast.Str(s='console')], keywords=[]
                )
                query_node = ast.Call(
                    func=ast.Attribute(value=query_node, attr='start', ctx=ast.Load()),
                    args=[], keywords=[]
                )

                # Create a list of statements: the query assignment and awaitTermination
                query_assign = ast.Assign(
                    targets=[ast.Name(id='query', ctx=ast.Store())],
                    value=query_node
                )
                
                await_termination = ast.Expr(
                    value=ast.Call(
                        func=ast.Attribute(
                            value=ast.Name(id='query', ctx=ast.Load()),
                            attr='awaitTermination',
                            ctx=ast.Load()
                        ),
                        args=[],
                        keywords=[]
                    )
                )
                
                return [query_assign, await_termination]
        return node

# ===== MAIN EXECUTION =====

parser = argparse.ArgumentParser(description="Convert batch.py to stream.py using AST")
parser.add_argument("input_file", help="Path to batch.py")
args = parser.parse_args()

with open(args.input_file, "r") as f:
    batch_code = f.read()

tree = ast.parse(batch_code)
transformer = BatchToStreamTransformer()
transformed_tree = transformer.visit(tree)
stream_logic = astor.to_source(transformed_tree)

with open("stream.py", "w") as f:
    f.write(stream_logic)

print("Final working stream.py generated successfully!")
