# DynamoDbLite

A lightweight implementation of the AWS DynamoDB client interface backed by SQLite.

## Overview

DynamoDbLite provides a drop-in replacement for the AWS DynamoDB SDK client, using SQLite as the storage engine. This enables:

- **Local development** without requiring AWS credentials or internet connectivity
- **Fast unit and integration testing** with an in-memory or file-based database
- **Reduced costs** during development by avoiding DynamoDB provisioned capacity charges
- **Offline functionality** for applications that need DynamoDB-like behavior without cloud dependencies

## Features

- Implements the AWS DynamoDB client interface
- SQLite-backed storage (in-memory or persistent)
- Support for core DynamoDB operations
- Compatible with existing code using the AWS SDK
