[![.NET Tests](https://github.com/marklauter/DynamoDbLite/actions/workflows/dotnet.tests.yml/badge.svg)](https://github.com/marklauter/DynamoDbLite/actions/workflows/dotnet.tests.yml)
[![.NET Publish](https://github.com/marklauter/DynamoDbLite/actions/workflows/dotnet.publish.yml/badge.svg)](https://github.com/marklauter/DynamoDbLite/actions/workflows/dotnet.publish.yml)
[![NuGet](https://img.shields.io/nuget/v/MSL.DynamoDbLite?logo=nuget)](https://www.nuget.org/packages/MSL.DynamoDbLite/)
[![.NET](https://img.shields.io/badge/.NET-10.0-blue)](https://dotnet.microsoft.com/en-us/download/dotnet/10.0/)

![MSL Armory](https://raw.githubusercontent.com/marklauter/DynamoDbLite/main/images/msl.armory.small.png "MSL Armory")

# DynamoDbLite

*Another weapon from the MSL Armory*

A lightweight implementation of the AWS DynamoDB client interface backed by SQLite.

## Overview

DynamoDbLite provides a drop-in replacement for the AWS DynamoDB SDK client, using SQLite as the storage engine. This enables:

- **Local development** without requiring AWS credentials or internet connectivity
- **Fast unit and integration testing** with an in-memory or file-based database
- **Reduced costs** during development by avoiding DynamoDB provisioned capacity charges
- **Offline functionality** for applications that need DynamoDB-like behavior without cloud dependencies

## Install

```shell
dotnet add package MSL.DynamoDbLite
```

The package id carries the publisher prefix; the assembly and namespace are `DynamoDbLite`.

```csharp
using DynamoDbLite;

// in-memory, isolated per test run
using var client = new DynamoDbClient(new DynamoDbLiteOptions(
    $"Data Source=app_{Guid.NewGuid():N};Mode=Memory;Cache=Shared"));
```

Or register it for DI:

```csharp
builder.Services.AddDynamoDbLite(o =>
    o.WithConnectionString("Data Source=myapp.db"));
```

A connection string is required; there is no default. See [Getting started](https://github.com/marklauter/DynamoDbLite/wiki/Getting-Started).

## Features

- **Item CRUD** with `ConditionExpression`, `ProjectionExpression`, `UpdateExpression`, and `ReturnValues`
- **Querying** with `KeyConditionExpression`, `FilterExpression`, sort-key ordering (string and numeric), pagination, and `Select.COUNT`
- **Batch** operations: `BatchGetItem`, `BatchWriteItem` (single transaction)
- **Paginators**: `client.Paginators` over all seven paged operations, single-use like the AWS SDK's own
- **Transactions**: `TransactWriteItems` and `TransactGetItems` with all-or-nothing semantics, `ClientRequestToken` idempotency, and `ReturnValuesOnConditionCheckFailure`
- **Secondary indexes**: GSI and LSI with sparse-index support, projection types `ALL`/`KEYS_ONLY`/`INCLUDE`, and `UpdateTable` GSI create/delete with backfill
- **TTL**: `UpdateTimeToLive`, `DescribeTimeToLive`, read-time filtering, inline sweep throttled per table
- **Tags**: `TagResource`, `UntagResource`, `ListTagsOfResource`
- **Export & Import**: file-system-backed analog of S3, `DYNAMODB_JSON` format
- **DynamoDbContext compatibility**: works with the AWS SDK high-level ORM (object persistence, `[DynamoDBVersion]` optimistic locking, GSI queries)
- **Two storage modes**: in-memory for fast tests; file-based with optional WAL for persistence
- **AWS SDK v4** (`AWSSDK.DynamoDBv2` 4.0+)

For the operation-by-operation support matrix and limitations, see the [API Parity](https://github.com/marklauter/DynamoDbLite/wiki/API-Parity) wiki page.

## Documentation

- [Wiki](https://github.com/marklauter/DynamoDbLite/wiki) — usage guide, API reference, and behavior notes
- [Decisions](docs/decisions/) — design rationale and phase status
- [API Parity](https://github.com/marklauter/DynamoDbLite/wiki/API-Parity) — what's supported, what's stubbed, what's out of scope
