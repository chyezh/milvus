#!/usr/bin/env python3
"""Replace nil/context.TODO() in log calls with ctx from enclosing function scope.

Strategy:
1. For each Go file, find function/closure boundaries by tracking brace depth.
2. For each scope, determine if a ctx variable is available (parameter, local, or captured).
3. Within scopes that have ctx: replace nil/context.TODO() → ctx
4. Within scopes without ctx: replace nil → context.TODO() (minimum safety fix)
"""

import re
import os
import sys

LOG_METHODS = r"(?:Info|Warn|Error|Debug|Log|RatedInfo|RatedWarn|RatedError|RatedDebug)"
LOG_CALL_RE = re.compile(
    r"(\." + LOG_METHODS + r"\()(\s*)(context\.TODO\(\)|nil)(\s*,)"
)


def strip_strings_and_line_comments(line):
    """Remove string literals and // comments for safe brace counting."""
    result = []
    i = 0
    n = len(line)
    while i < n:
        ch = line[i]
        # Line comment
        if ch == "/" and i + 1 < n and line[i + 1] == "/":
            break
        # Double-quoted string
        if ch == '"':
            i += 1
            while i < n and line[i] != '"':
                if line[i] == "\\":
                    i += 1  # skip escaped char
                i += 1
            i += 1  # skip closing quote
            continue
        # Backtick raw string (may span lines, but we only process one line)
        if ch == "`":
            i += 1
            while i < n and line[i] != "`":
                i += 1
            i += 1
            continue
        # Rune literal
        if ch == "'":
            i += 1
            if i < n and line[i] == "\\":
                i += 1
            if i < n:
                i += 1  # char
            if i < n and line[i] == "'":
                i += 1
            continue
        result.append(ch)
        i += 1
    return "".join(result)


def find_ctx_in_signature(sig_text):
    """Extract ctx variable name from function signature."""
    m = re.search(r"\b(\w+)\s+context\.Context\b", sig_text)
    if m:
        return m.group(1)
    return None


def process_file(filepath):
    """Process a single Go file. Returns (modified, stats_dict)."""
    with open(filepath) as f:
        content = f.read()

    if not LOG_CALL_RE.search(content):
        return False, {}

    lines = content.split("\n")

    # Scope stack: [(target_brace_depth, ctx_name_or_None)]
    # target_brace_depth = the depth when this scope's closing } will be hit
    scope_stack = []
    brace_depth = 0
    in_block_comment = False
    replacements_ctx = 0
    replacements_todo = 0
    nil_to_todo = 0

    for i in range(len(lines)):
        raw_line = lines[i]

        # Handle block comments
        line_for_parse = raw_line
        if in_block_comment:
            end_idx = line_for_parse.find("*/")
            if end_idx >= 0:
                in_block_comment = False
                line_for_parse = line_for_parse[end_idx + 2 :]
            else:
                # Entire line is comment
                continue

        # Remove block comment segments within this line
        while True:
            start_idx = line_for_parse.find("/*")
            if start_idx < 0:
                break
            end_idx = line_for_parse.find("*/", start_idx + 2)
            if end_idx >= 0:
                line_for_parse = (
                    line_for_parse[:start_idx] + line_for_parse[end_idx + 2 :]
                )
            else:
                line_for_parse = line_for_parse[:start_idx]
                in_block_comment = True
                break

        clean = strip_strings_and_line_comments(line_for_parse)

        # Detect function/closure declarations
        # Match: func keyword (not in a comment)
        stripped = clean.lstrip()
        is_func_decl = False
        if re.match(r"(?:go\s+)?func\b", stripped):
            is_func_decl = True
        elif re.search(r":?=\s*func\s*\(", clean):
            is_func_decl = True
        elif re.search(r",\s*func\s*\(", clean):
            is_func_decl = True

        if is_func_decl:
            # Collect full signature (may span multiple lines)
            sig = raw_line
            j = i
            while (
                "{" not in strip_strings_and_line_comments(sig) and j < len(lines) - 1
            ):
                j += 1
                sig += " " + lines[j]

            ctx_name = find_ctx_in_signature(sig)

            # For closures without ctx param, inherit from outer scope
            if ctx_name is None:
                for _, outer_ctx in reversed(scope_stack):
                    if outer_ctx:
                        ctx_name = outer_ctx
                        break

            # Push scope when we see the opening brace
            opens = clean.count("{")
            if opens > 0:
                scope_stack.append((brace_depth + 1, ctx_name))

        # Check for ctx assignment in body: ctx := ... or ctx, cancel := ...
        ctx_assign = re.search(r"\bctx\s*(?::=|,\s*\w+\s*:?=)", clean)
        if ctx_assign and scope_stack and scope_stack[-1][1] is None:
            scope_stack[-1] = (scope_stack[-1][0], "ctx")

        # Determine current ctx from innermost scope
        current_ctx = None
        for _, ctx_name in reversed(scope_stack):
            if ctx_name:
                current_ctx = ctx_name
                break

        # Perform replacements on this line
        def replacer(m):
            nonlocal replacements_ctx, replacements_todo, nil_to_todo
            prefix = m.group(1)  # .Info(
            ws1 = m.group(2)  # whitespace before arg
            old_arg = m.group(3)  # context.TODO() or nil
            ws2 = m.group(4)  # , (with optional whitespace)

            if current_ctx:
                replacements_ctx += 1
                return prefix + ws1 + current_ctx + ws2
            elif old_arg == "nil":
                nil_to_todo += 1
                return prefix + ws1 + "context.TODO()" + ws2
            else:
                # Already context.TODO() and no ctx available, leave as is
                return m.group(0)

        new_line = LOG_CALL_RE.sub(replacer, raw_line)
        lines[i] = new_line

        # Update brace depth
        opens = clean.count("{")
        closes = clean.count("}")
        brace_depth += opens - closes

        # Pop scopes that have ended
        while scope_stack and brace_depth < scope_stack[-1][0]:
            scope_stack.pop()

    new_content = "\n".join(lines)
    if new_content != content:
        with open(filepath, "w") as f:
            f.write(new_content)
        return True, {
            "ctx": replacements_ctx,
            "nil_to_todo": nil_to_todo,
        }
    return False, {}


def main():
    root = sys.argv[1] if len(sys.argv) > 1 else "."
    skip_dirs = {"vendor", "cmake_build", ".git", ".claude"}

    total_files = 0
    total_ctx = 0
    total_nil_to_todo = 0

    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in skip_dirs]

        # Skip pkg/log/ (the log library itself) but allow pkg/log/logcore/
        rel = os.path.relpath(dirpath, root)
        if rel == os.path.join("pkg", "log"):
            dirnames[:] = [d for d in dirnames if d == "logcore"]
            continue

        for filename in filenames:
            if not filename.endswith(".go"):
                continue
            filepath = os.path.join(dirpath, filename)
            modified, stats = process_file(filepath)
            if modified:
                total_files += 1
                total_ctx += stats.get("ctx", 0)
                total_nil_to_todo += stats.get("nil_to_todo", 0)
                print(f"  {os.path.relpath(filepath, root)}")

    print(f"\nDone: {total_files} files modified")
    print(f"  Replaced with ctx: {total_ctx}")
    print(f"  nil → context.TODO() (no ctx): {total_nil_to_todo}")


if __name__ == "__main__":
    main()
