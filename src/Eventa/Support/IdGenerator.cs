using static pblasucci.Ananoid.KnownAlphabets;

namespace Eventa;

/// <summary>
/// Generates Eventa-compatible identifiers using the same alphanumeric format as the
/// TypeScript implementation.
/// </summary>
public static class IdGenerator
{
    /// <summary>
    /// Creates a new identifier with the requested length.
    /// </summary>
    /// <param name="size">The number of characters to generate. Defaults to 16.</param>
    /// <returns>A new alphanumeric identifier.</returns>
    public static string New(int size = 16)
    {
        return Alphanumeric.MakeNanoId(size).ToString();
    }
}
