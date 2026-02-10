---
name: format
description: Format the entire solution using dotnet format (whitespace, code style, and analyzers).
allowed-tools: Bash
---

# Format Solution

Run `dotnet format "src/DynamoDbLite.slnx" --verbosity normal` to format the entire solution using the `.editorconfig` settings. Show the output to me.

Note: The `format.cmd` wrapper was removed. Always use the dotnet command above directly.
