namespace DynamoDbLite;

/// <summary>
/// Validates SQLite pragma name/value pairs supplied via <see cref="DynamoDbLiteOptionsBuilder.WithPragma"/>
/// (or <see cref="DynamoDbLiteOptions.Pragmas"/>). Pragma values cannot be parameterized — they are concatenated
/// into <c>PRAGMA name=value;</c> — so both parts are constrained to characters that cannot terminate the
/// statement or inject SQL. This is an injection-safety boundary, not a semantic allow-list: any pragma name is
/// accepted, and any integer or keyword value. Anything that does not fit (string-valued pragmas, custom setup)
/// belongs in <see cref="DynamoDbLiteOptions.ConnectionInitializer"/>, which runs arbitrary code.
/// </summary>
internal static class PragmaValidator
{
    /// <summary>
    /// Validates a pragma name and value, throwing <see cref="DynamoDbLiteConfigurationException"/> on a value
    /// that could break out of <c>PRAGMA name=value;</c>.
    /// </summary>
    /// <param name="name">A pragma name: a SQLite identifier matching <c>[A-Za-z_][A-Za-z0-9_]*</c>.</param>
    /// <param name="value">A pragma value: a signed integer (e.g. <c>5000</c>, <c>-16000</c>) or a bare keyword
    /// (e.g. <c>NORMAL</c>, <c>WAL</c>, <c>ON</c>) matching <c>-?[A-Za-z0-9_]+</c>.</param>
    /// <exception cref="DynamoDbLiteConfigurationException">The name or value is null, empty, or contains characters outside the allowed set.</exception>
    internal static void Validate(string name, string value)
    {
        if (!IsValidName(name))
        {
            throw new DynamoDbLiteConfigurationException(
                $"Invalid pragma name: '{name ?? "<null>"}'. Names must match [A-Za-z_][A-Za-z0-9_]*. " +
                "For setup that doesn't fit a simple PRAGMA name=value, use WithConnectionInitializer.");
        }

        if (!IsValidValue(value))
        {
            throw new DynamoDbLiteConfigurationException(
                $"Invalid pragma value for '{name}': '{value ?? "<null>"}'. Values must be a signed integer or a " +
                "bare keyword (matching -?[A-Za-z0-9_]+); pragma values cannot be parameterized. For string-valued " +
                "pragmas or custom setup, use WithConnectionInitializer.");
        }
    }

    private static bool IsValidName(string name)
    {
        if (string.IsNullOrEmpty(name))
            return false;

        if (!IsLetterOrUnderscore(name[0]))
            return false;

        for (var i = 1; i < name.Length; i++)
        {
            if (!IsLetterDigitOrUnderscore(name[i]))
                return false;
        }

        return true;
    }

    private static bool IsValidValue(string value)
    {
        if (string.IsNullOrEmpty(value))
            return false;

        var start = value[0] == '-' ? 1 : 0;
        if (start == value.Length) // a lone '-'
            return false;

        for (var i = start; i < value.Length; i++)
        {
            if (!IsLetterDigitOrUnderscore(value[i]))
                return false;
        }

        return true;
    }

    private static bool IsLetterOrUnderscore(char c) =>
        c is >= 'A' and <= 'Z' or >= 'a' and <= 'z' or '_';

    private static bool IsLetterDigitOrUnderscore(char c) =>
        IsLetterOrUnderscore(c) || c is >= '0' and <= '9';
}
