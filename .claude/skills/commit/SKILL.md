---
name: commit
description: Create a git commit with a well-formatted message
disable-model-invocation: true
---

# Git Commit

Create a git commit with a well-formatted commit message following Milvus conventions.

## Usage

```
/commit [message]
```

If no message is provided, analyze the staged changes and generate an appropriate commit message.

## Instructions

1. Run `git status` to see current changes
2. Run `git diff --cached` to see staged changes (if any)
3. Run `git diff` to see unstaged changes
4. Analyze the changes to understand what was modified
5. If user provided a message, use it; otherwise generate one based on changes
6. Stage relevant files if needed (prefer specific files over `git add -A`)
7. Create the commit with format:

```
<type>: <short description>

<optional body explaining why/what>

Co-Authored-By: Claude <noreply@anthropic.com>
```

## Commit Types

- `feat`: New feature
- `fix`: Bug fix
- `enhance`: Enhancement to existing feature
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `docs`: Documentation changes
- `chore`: Maintenance tasks

## Example

```
/commit fix: resolve race condition in segment loading
```
