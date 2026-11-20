using System.Diagnostics.CodeAnalysis;
using System.Net.Http.Headers;
using System.Text;

namespace Eventa;

// Adapts the BCL parser to RFC 9110 media type syntax and Eventa's semantic lookup.
internal static class DeliveryContentType
{
    /// <summary>Checks whether a string is a valid media type for a delivery.</summary>
    /// <param name="value">The original content-type string to validate.</param>
    /// <returns><see langword="true"/> if the media type is valid.</returns>
    public static bool IsValid(string value) => TryParse(value, out _);

    /// <summary>Compares the parsed type and subtype using ordinal, case-insensitive comparison.</summary>
    /// <param name="value">The content-type string to parse, including any parameters.</param>
    /// <param name="mediaType">The expected type and subtype without parameters.</param>
    /// <returns><see langword="true"/> if the string is valid and its type and subtype match.</returns>
    public static bool IsMediaType(string value, string mediaType)
    {
        return TryParse(value, out MediaTypeHeaderValue? parsed)
            && string.Equals(parsed.MediaType, mediaType, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Looks up the first parameter with the specified name, ignoring case in the name.</summary>
    /// <param name="value">The content-type string to parse.</param>
    /// <param name="name">The parameter name to find.</param>
    /// <returns>
    /// The parameter value with quoting and escapes removed and original casing preserved, or
    /// <see langword="null"/> if the media type is invalid or the parameter is absent.
    /// </returns>
    public static string? GetParameter(string value, string name)
    {
        if (!TryParse(value, out MediaTypeHeaderValue? parsed)) return null;

        // Preserve the first matching parameter and leave its value's casing intact.
        foreach (NameValueHeaderValue parameter in parsed.Parameters)
        {
            if (!string.Equals(parameter.Name, name, StringComparison.OrdinalIgnoreCase)) continue;

            return Unquote(parameter.Value!);
        }

        return null;
    }

    private static bool TryParse(string value, [NotNullWhen(true)] out MediaTypeHeaderValue? parsed)
    {
        parsed = null;
        string? parseInput = PrepareForParsing(value);
        if (parseInput is null) return false;
        if (!MediaTypeHeaderValue.TryParse(parseInput, out MediaTypeHeaderValue? header)) return false;

        // The BCL also accepts bare names, which RFC 9110 parameter syntax excludes.
        foreach (NameValueHeaderValue parameter in header.Parameters)
        {
            if (parameter.Value is null) return false;
        }

        parsed = header;
        return true;
    }

    // Check the syntax the BCL tolerates, and omit empty parameter segments from its input only.
    // Delivery retains the original string, including any delimiters omitted here.
    private static string? PrepareForParsing(string value)
    {
        // A standalone media type begins with the type token.
        if (value.Length == 0 || IsWhitespace(value[0])) return null;

        StringBuilder? parseInput = null;
        int segmentStart = 0;
        bool inQuotes = false;
        for (int index = 0; index < value.Length; index++)
        {
            char character = value[index];
            if (!IsAllowedCharacter(character)) return null;

            if (inQuotes && character == '\\')
            {
                // An escaped quote is part of the value and cannot close it.
                if (++index == value.Length || !IsAllowedCharacter(value[index])) return null;

                continue;
            }

            if (character == '"')
            {
                inQuotes = !inQuotes;
                continue;
            }

            if (inQuotes) continue;

            // The semicolon branch consumes any whitespace after an empty final parameter.
            if (index == value.Length - 1 && IsWhitespace(character)) return null;

            // RFC 9110 does not allow whitespace around type/subtype or name=value separators.
            var hasWhitespaceBefore = index > 0 && IsWhitespace(value[index - 1]);
            var hasWhitespaceAfter = index + 1 < value.Length && IsWhitespace(value[index + 1]);
            if (character is '/' or '=' && (hasWhitespaceBefore || hasWhitespaceAfter)) return null;

            if (character == ';')
            {
                int next = index + 1;
                while (next < value.Length && IsWhitespace(value[next]))
                {
                    next++;
                }

                // RFC 9110 allows empty parameter segments between semicolons and at the end.
                if (next == value.Length || value[next] == ';')
                {
                    parseInput ??= new StringBuilder();
                    parseInput.Append(value.AsSpan(segmentStart, index - segmentStart));
                    segmentStart = next;
                }

                index = next - 1;
            }
        }

        return inQuotes ? null : parseInput?.Append(value.AsSpan(segmentStart)).ToString() ?? value;
    }

    private static bool IsAllowedCharacter(char value) =>
        value is '\t' or (>= ' ' and <= '~') or (>= '\u0080' and <= '\u00ff');

    private static bool IsWhitespace(char value) => value is ' ' or '\t';

    private static string Unquote(string value)
    {
        if (value[0] != '"') return value;

        // NameValueHeaderValue exposes the original quotes and backslash escapes.
        ReadOnlySpan<char> inner = value.AsSpan(1, value.Length - 2);
        if (!inner.Contains('\\')) return inner.ToString();

        StringBuilder unescaped = new(inner.Length);
        for (int index = 0; index < inner.Length; index++)
        {
            if (inner[index] == '\\')
            {
                index++;
            }

            unescaped.Append(inner[index]);
        }

        return unescaped.ToString();
    }
}
