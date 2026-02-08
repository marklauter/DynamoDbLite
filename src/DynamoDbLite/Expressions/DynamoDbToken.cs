namespace DynamoDbLite.Expressions;

internal enum DynamoDbToken
{
    // Literals & identifiers
    Identifier,
    ExpressionAttrName,   // #name
    ExpressionAttrValue,  // :value
    Number,               // integer literal (for list indexes)

    // Operators
    Equal,                // =
    NotEqual,             // <>
    LessThan,             // <
    LessThanOrEqual,      // <=
    GreaterThan,          // >
    GreaterThanOrEqual,   // >=

    // Punctuation
    Comma,                // ,
    Dot,                  // .
    OpenParen,            // (
    CloseParen,           // )
    OpenBracket,          // [
    CloseBracket,         // ]
    Plus,                 // +
    Minus,                // -

    // Keywords
    And,
    Or,
    Not,
    Between,
    In,
    Set,
    Remove,
    Add,
    Delete,

    // Functions
    AttributeExists,
    AttributeNotExists,
    AttributeType,
    BeginsWith,
    Contains,
    Size,
    IfNotExists,
    ListAppend,
}
