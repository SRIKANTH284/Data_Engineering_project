import ast
import astor
import argparse

class BatchToStreamTransformer(ast.NodeTransformer):
    def __init__(self):
        super().__init__()
        self.df_name = None

    def visit_Module(self, node):
        # Add StreamingContext import
        streaming_import = ast.ImportFrom(
            module='pyspark.streaming',
            names=[ast.alias(name='StreamingContext', asname=None)],
            level=0
        )
        node.body.insert(0, streaming_import)
        return self.generic_visit(node)

    def visit_Assign(self, node):
        # Add StreamingContext after sparkContext
        if (isinstance(node.value, ast.Attribute) and 
            node.value.attr == 'sparkContext' and
            isinstance(node.value.value, ast.Name) and
            node.value.value.id == 'spark'):
            
            ssc_assign = ast.Assign(
                targets=[ast.Name(id='ssc', ctx=ast.Store())],
                value=ast.Call(
                    func=ast.Name(id='StreamingContext', ctx=ast.Load()),
                    args=[
                        ast.Name(id='sc', ctx=ast.Load()),
                        ast.Num(n=5)
                    ],
                    keywords=[]
                )
            )
            return [node, ssc_assign]

        # Change textFile to textFileStream and target to lines
        if isinstance(node.value, ast.Call) and node.value.func.attr == "textFile":
            node.value.func.value = ast.Name(id='ssc', ctx=ast.Load())
            node.value.func.attr = "textFileStream"
            node.value.args = [ast.Str(s='stream_input/')]
            node.targets = [ast.Name(id='lines', ctx=ast.Store())]
            return node

        # Convert the processing chain to streaming
        if isinstance(node.targets[0], ast.Name) and node.targets[0].id == "rdd_filtered":
            # Create the streaming processing chain
            processing_chain = ast.Call(
                func=ast.Attribute(
                    value=ast.Name(id='lines', ctx=ast.Load()),
                    attr='foreachRDD',
                    ctx=ast.Load()
                ),
                args=[
                    ast.Lambda(
                        args=ast.arguments(
                            args=[ast.arg(arg='rdd', annotation=None)],
                            vararg=None,
                            kwonlyargs=[],
                            kw_defaults=[],
                            kwarg=None,
                            defaults=[]
                        ),
                        body=ast.Call(
                            func=ast.Attribute(
                                value=node.value,
                                attr='foreach',
                                ctx=ast.Load()
                            ),
                            args=[
                                ast.Lambda(
                                    args=ast.arguments(
                                        args=[ast.arg(arg='result', annotation=None)],
                                        vararg=None,
                                        kwonlyargs=[],
                                        kw_defaults=[],
                                        kwarg=None,
                                        defaults=[]
                                    ),
                                    body=ast.Call(
                                        func=ast.Name(id='print', ctx=ast.Load()),
                                        args=[ast.Name(id='result', ctx=ast.Load())],
                                        keywords=[]
                                    )
                                )
                            ],
                            keywords=[]
                        )
                    )
                ],
                keywords=[]
            )
            return ast.Expr(value=processing_chain)

            
        return node

    def visit_Expr(self, node):
        # Replace saveAsTextFile with start and awaitTermination
        if isinstance(node.value, ast.Call) and node.value.func.attr == "saveAsTextFile":
            start_call = ast.Expr(
                value=ast.Call(
                    func=ast.Attribute(
                        value=ast.Name(id='ssc', ctx=ast.Load()),
                        attr='start',
                        ctx=ast.Load()
                    ),
                    args=[],
                    keywords=[]
                )
            )
            
            await_call = ast.Expr(
                value=ast.Call(
                    func=ast.Attribute(
                        value=ast.Name(id='ssc', ctx=ast.Load()),
                        attr='awaitTermination',
                        ctx=ast.Load()
                    ),
                    args=[],
                    keywords=[]
                )
            )
            
            return [start_call, await_call]  # Replace the saveAsTextFile line with these two calls
            
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