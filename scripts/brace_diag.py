"""Debug the brace imbalance in job-monitor.php"""
import re
import sys
from pathlib import Path

src = Path(sys.argv[1]).read_text(encoding="utf-8")

# 1. Remove heredoc / nowdoc blocks
src2 = re.sub(
    r"<<<(['\"]?)(\w+)\1\s*\n.*?\n\s*\2\s*;?",
    '""',
    src,
    flags=re.DOTALL,
)
# 2. Remove /* … */ block comments
src2 = re.sub(r"/\*.*?\*/", "", src2, flags=re.DOTALL)
# 3. Remove // and # line comments
src2 = re.sub(r"(?m)(//|#)[^\n]*", "", src2)
# 4. Remove double-quoted strings
src2 = re.sub(r'"(?:\\.|[^"\\])*"', '""', src2)
# 5. Remove single-quoted strings
src2 = re.sub(r"'(?:\\.|[^'\\])*'", "''", src2)

opens = src2.count("{")
closes = src2.count("}")
print(f"after strip: {{={opens} }}={closes} diff={opens - closes}")

# Walk line by line, tracking running balance, print where delta changes
depth = 0
for i, line in enumerate(src2.split("\n"), 1):
    o = line.count("{")
    c = line.count("}")
    delta = o - c
    if delta != 0:
        depth += delta
        if i % 50 == 0 or abs(depth) > 2:
            # noisy, skip
            pass
# Show where unbalanced lines are
print("\nLines with net unbalanced braces (top 20):")
imbalanced = []
for i, line in enumerate(src2.split("\n"), 1):
    o = line.count("{")
    c = line.count("}")
    if o != c:
        imbalanced.append((i, o - c, line.strip()[:100]))
# Show first 10 and last 10
for i, d, text in imbalanced[:10]:
    print(f"  L{i}: delta={d:+d}  {text!r}")
print("...")
for i, d, text in imbalanced[-10:]:
    print(f"  L{i}: delta={d:+d}  {text!r}")
