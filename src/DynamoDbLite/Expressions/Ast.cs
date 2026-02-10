using Amazon.DynamoDBv2.Model;

namespace DynamoDbLite.Expressions;

// ── Path elements ──────────────────────────────────────────────────────

internal abstract record PathElement;

internal sealed record AttributeNameElement(string Name)
    : PathElement;

internal sealed record ListIndexElement(int Index)
    : PathElement;

internal sealed record AttributePath(IReadOnlyList<PathElement> Elements);

// ── Condition AST ──────────────────────────────────────────────────────

internal abstract record ConditionNode;

internal sealed record ComparisonNode(
    Operand Left,
    string Operator,
    Operand Right)
    : ConditionNode;

internal sealed record BetweenNode(
    Operand Value,
    Operand Lower,
    Operand Upper)
    : ConditionNode;

internal sealed record InNode(
    Operand Value,
    IReadOnlyList<Operand> List)
    : ConditionNode;

internal sealed record LogicalNode(
    ConditionNode Left,
    string Operator,
    ConditionNode Right)
    : ConditionNode;

internal sealed record NotNode(ConditionNode Inner)
    : ConditionNode;

internal sealed record FunctionConditionNode(
    string FunctionName,
    IReadOnlyList<Operand> Arguments)
    : ConditionNode;

// ── Operands ───────────────────────────────────────────────────────────

internal abstract record Operand;

internal sealed record PathOperand(AttributePath Path)
    : Operand;

internal sealed record ValueRefOperand(string ValueRef)
    : Operand;

internal sealed record SizeFunctionOperand(AttributePath Path)
    : Operand;

internal sealed record LiteralOperand(AttributeValue Value)
    : Operand;

// ── Update AST ─────────────────────────────────────────────────────────

internal sealed record UpdateExpression(
    IReadOnlyList<SetAction> Sets,
    IReadOnlyList<RemoveAction> Removes,
    IReadOnlyList<AddAction> Adds,
    IReadOnlyList<DeleteAction> Deletes);

internal sealed record SetAction(
    AttributePath Path,
    UpdateValue Value);

internal sealed record RemoveAction(AttributePath Path);

internal sealed record AddAction(
    AttributePath Path,
    string ValueRef);

internal sealed record DeleteAction(
    AttributePath Path,
    string ValueRef);

// ── Update values ──────────────────────────────────────────────────────

internal abstract record UpdateValue;

internal sealed record PathUpdateValue(AttributePath Path)
    : UpdateValue;

internal sealed record ValueRefUpdateValue(string ValueRef)
    : UpdateValue;

internal sealed record ArithmeticUpdateValue(
    UpdateValue Left,
    string Operator,
    UpdateValue Right)
    : UpdateValue;

internal sealed record IfNotExistsUpdateValue(
    AttributePath Path,
    UpdateValue Default)
    : UpdateValue;

internal sealed record ListAppendUpdateValue(
    UpdateValue First,
    UpdateValue Second)
    : UpdateValue;

// ── Key Condition AST ─────────────────────────────────────────────────

internal sealed record KeyCondition(
    PartitionKeyCondition PartitionKey,
    SortKeyCondition? SortKey);

internal sealed record PartitionKeyCondition(
    Operand KeyPath,
    Operand Value);

internal abstract record SortKeyCondition;

internal sealed record SortKeyComparisonCondition(
    Operand KeyPath,
    string Operator,
    Operand Value)
    : SortKeyCondition;

internal sealed record SortKeyBetweenCondition(
    Operand KeyPath,
    Operand Lower,
    Operand Upper)
    : SortKeyCondition;

internal sealed record SortKeyBeginsWithCondition(
    Operand KeyPath,
    Operand Prefix)
    : SortKeyCondition;
