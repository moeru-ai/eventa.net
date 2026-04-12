using static pblasucci.Ananoid.KnownAlphabets;

namespace Eventa;

public static class IdGenerator
{
    public static string New(int size = 16)
    {
        // Match Eventa TypeScript IDs: 16-char alphanumeric IDs using a custom alphabet.
        return Alphanumeric.MakeNanoId(size).ToString();
    }
}
