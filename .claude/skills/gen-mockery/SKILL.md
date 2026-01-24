---
name: gen-mockery
description: Regenerate mockery mocks for a specific Milvus component
disable-model-invocation: true
---

# Generate Mockery Mocks

Regenerate mockery mocks for the specified Milvus component.

## Usage

```
/gen-mockery <component>
```

## Available Components

- `querynode` - QueryNode component mocks
- `datacoord` - DataCoord component mocks
- `proxy` - Proxy component mocks
- `rootcoord` - RootCoord component mocks
- `querycoord` - QueryCoord component mocks
- `datanode` - DataNode component mocks
- `streaming` - Streaming component mocks
- `all` - Regenerate all mocks

## Instructions

1. Identify the component from user input
2. Run the appropriate make command:
   - For specific component: `make generate-mockery-<component>`
   - For all mocks: `make generate-mockery`
3. Report any errors encountered

## Example

```
/gen-mockery datacoord
```

This will run `make generate-mockery-datacoord` to regenerate DataCoord mocks.
