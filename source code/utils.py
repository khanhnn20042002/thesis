import time
import json
from random import randint
import ast

def get_tickers():
    return json.load(open("tickers.json"))

def get_fixed_indicators():
    return ["SMA", "ADL", "Aroon", "EMA", "MACD", "OBV", "RSI"]

def try_until_success(func, seconds, exception, max_attempt, *args, **kwargs):
    attempt = 0
    while True:
        attempt += 1
        try:
            print(f"Attempt: {attempt}. Running {func.__name__}")
            return func(*args, **kwargs)
        except exception as e:
            print(f"Exception occurred: {e}. Retrying in {seconds} seconds...")
            time.sleep(seconds)
        else:
            break
        if attempt == max_attempt:
            break

def mix_keep_internal_order(buckets):
    buckets = [bucket for bucket in buckets if len(bucket) != 0]
    result = []
    num_buckets = len(buckets)
    num_elements = sum([len(bucket) for bucket in buckets])
    for i in range(num_elements):
        random_bucket = buckets[randint(0, num_buckets - 1)]
        result.append(random_bucket.pop(0))
        if(len(random_bucket) == 0):
            num_buckets -= 1
            buckets.remove(random_bucket)
    return result

def validate_python_syntax(code):
    try:
        ast.parse(code)
        return True
    except SyntaxError:
        return False

def check_variable_in_code(code, variable_name):
    class VariableVisitor(ast.NodeVisitor):
        def __init__(self):
            self.found = False

        def visit_Name(self, node):
            if isinstance(node.ctx, ast.Store) and node.id == variable_name:
                self.found = True

    try:
        tree = ast.parse(code)
        visitor = VariableVisitor()
        visitor.visit(tree)
        return visitor.found
    except SyntaxError:
        return False

if __name__ == '__main__':
    def my_code(a, b, c=0):
        print(f"Attempting to run code with arguments: a={a}, b={b}, c={c}")
        if a + b + c < 10:
            raise ValueError("Simulated error")

    try_until_success(my_code, 2, (ValueError, TypeError), 10,  3, 4, c=2)

