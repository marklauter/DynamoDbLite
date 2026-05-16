![DynamoDbLite Logo](https://raw.githubusercontent.com/marklauter/DynamoDbLite/refs/heads/main/images/dynamodblite-logo-2.png)

# DynamoDbLite

[![.NET Tests](https://github.com/marklauter/DynamoDbLite/actions/workflows/dotnet.tests.yml/badge.svg)](https://github.com/marklauter/DynamoDbLite/actions/workflows/dotnet.tests.yml)
[![.NET](https://img.shields.io/badge/.NET-10.0-blue)](https://dotnet.microsoft.com/en-us/download/dotnet/10.0/)

A lightweight implementation of the AWS DynamoDB client interface backed by SQLite.

## Overview

DynamoDbLite provides a drop-in replacement for the AWS DynamoDB SDK client, using SQLite as the storage engine. This enables:

- **Local development** without requiring AWS credentials or internet connectivity
- **Fast unit and integration testing** with an in-memory or file-based database
- **Reduced costs** during development by avoiding DynamoDB provisioned capacity charges
- **Offline functionality** for applications that need DynamoDB-like behavior without cloud dependencies

## Features

- **Item CRUD** with `ConditionExpression`, `ProjectionExpression`, `UpdateExpression`, and `ReturnValues`
- **Querying** with `KeyConditionExpression`, `FilterExpression`, sort-key ordering (string and numeric), pagination, and `Select.COUNT`
- **Batch** operations: `BatchGetItem`, `BatchWriteItem` (single transaction)
- **Transactions**: `TransactWriteItems` and `TransactGetItems` with all-or-nothing semantics, `ClientRequestToken` idempotency, and `ReturnValuesOnConditionCheckFailure`
- **Secondary indexes**: GSI and LSI with sparse-index support, projection types `ALL`/`KEYS_ONLY`/`INCLUDE`, and `UpdateTable` GSI create/delete with backfill
- **TTL**: `UpdateTimeToLive`, `DescribeTimeToLive`, read-time filtering, background cleanup
- **Tags**: `TagResource`, `UntagResource`, `ListTagsOfResource`
- **Export & Import**: file-system-backed analog of S3, `DYNAMODB_JSON` format
- **DynamoDbContext compatibility**: works with the AWS SDK high-level ORM (object persistence, `[DynamoDBVersion]` optimistic locking, GSI queries)
- **Two storage modes**: in-memory (default) for fast tests; file-based with WAL for persistence
- **AWS SDK v4** (`AWSSDK.DynamoDBv2` 4.0+)

For the operation-by-operation support matrix and limitations, see the [API Parity](https://github.com/marklauter/DynamoDbLite/wiki/API-Parity) wiki page.

## Documentation

- [Wiki](https://github.com/marklauter/DynamoDbLite/wiki) — usage guide, API reference, and behaviour notes
- [Architecture Decisions](docs/adrs/index.md) — design rationale and phase status
- [API Parity](https://github.com/marklauter/DynamoDbLite/wiki/API-Parity) — what's supported, what's stubbed, what's out of scope
