namespace Eventa;

public static class InvokeExtensions
{
    public static void RegisterAbortEvent(
        this IEventContext context,
        EventDefinition<object> fatalEvent)
    {
        throw new NotImplementedException();
    }
}
