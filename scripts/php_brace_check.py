"""Poor-man's PHP brace/paren balance check. Strips strings, comments, and
heredocs so literal braces inside them don't skew the count.

Usage: python scripts/php_brace_check.py <path>
"""
from __future__ import annotations

import re
import sys
from pathlib import Path


def strip_noise(src: str) -> str:
    # 1. Remove heredoc / nowdoc blocks: <<<'TAG' ... TAG;  or  <<<"TAG" ... TAG;
    src = re.sub(
        r"<<<(['\"]?)(\w+)\1\s*\n.*?\n\s*\2\s*;?",
        '""',
        src,
        flags=re.DOTALL,
    )
    # 2. Remove /* … */ block comments (non-greedy)
    src = re.sub(r"/\*.*?\*/", "", src, flags=re.DOTALL)
    # 3. Remove // and # line comments
    src = re.sub(r"(?m)(//|#)[^\n]*", "", src)
    # 4. Remove double-quoted strings (handle escapes)
    src = re.sub(r'"(?:\\.|[^"\\])*"', '""', src)
    # 5. Remove single-quoted strings
    src = re.sub(r"'(?:\\.|[^'\\])*'", "''", src)
    return src


def main(path: str) -> int:
    src = Path(path).read_text(encoding="utf-8")
    stripped = strip_noise(src)
    opens = stripped.count("{")
    closes = stripped.count("}")
    popens = stripped.count("(")
    pcloses = stripped.count(")")
    print(f"{path}")
    print(f"  braces:  {{={opens}  }}={closes}  diff={opens - closes}")
    print(f"  parens:  (={popens}  )={pcloses}  diff={popens - pcloses}")
    ok = (opens == closes) and (popens == pcloses)
    print("  status:", "OK" if ok else "IMBALANCED")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
