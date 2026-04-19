"""Stage-by-stage brace balance for job-monitor.php."""
import re
import sys
from pathlib import Path

src = Path(sys.argv[1]).read_text(encoding="utf-8")

def count(s, label):
    print(f"{label:30s} {{ = {s.count('{'):4d}   }} = {s.count('}'):4d}   diff = {s.count('{') - s.count('}'):+d}")

count(src, "raw")

# 1. Strip heredocs
s1 = re.sub(
    r"<<<(['\"]?)(\w+)\1\s*\n.*?\n\s*\2\s*;?",
    '""',
    src,
    flags=re.DOTALL,
)
count(s1, "after heredocs")

# 2. Strip block comments
s2 = re.sub(r"/\*.*?\*/", "", s1, flags=re.DOTALL)
count(s2, "after block comments")

# 3. Strip line comments
s3 = re.sub(r"(?m)(//|#)[^\n]*", "", s2)
count(s3, "after line comments")

# 4. Strip double-quoted strings
s4 = re.sub(r'"(?:\\.|[^"\\])*"', '""', s3)
count(s4, "after double strings")

# 5. Strip single-quoted strings
s5 = re.sub(r"'(?:\\.|[^'\\])*'", "''", s4)
count(s5, "after single strings")
