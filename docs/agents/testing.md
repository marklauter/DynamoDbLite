# Testing

## Philosophy

**Don't test what you don't own.** Tests exercise DynamoDbLite's behavior, not the AWS SDK or SQLite. No assertions on AWS SDK serialization, Dapper mapping, or raw SQL — focus on the contract: given a DynamoDB API call, does our implementation return the correct response?

**Test the contract, not the construction.** Assert on what a method promises, not how it does it. No assertions on private state, internal calls, or generated SQL — refactoring internals should not break tests.

**Tests are documentation.** Test names read like a spec. Underscored `Method_Scenario_Outcome` names are allowed in the test project (`CA1707` is suppressed there).

## Setup

Any project named `*.Tests` is auto-configured by [`src/Directory.Build.props`](../../src/Directory.Build.props). Create the project, add a `ProjectReference` to the system under test, and the test packages, global usings, and analyzer suppressions come along for free.
