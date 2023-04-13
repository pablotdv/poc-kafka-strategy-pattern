using System.Text.Json.Serialization;

namespace common;

public class BaseEvent
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
}

public class MessageEvent : BaseEvent
{
    public string Message { get; set; }
}

public class CallbackMessageEvent : BaseEvent
{
    public string CallbackMessage { get; set; }
}

public class CallbackStatusEvent : BaseEvent
{
    public string CallbackStatus { get; set; }
}

public class TransbordoEvent : BaseEvent
{
    public string Transbordo { get; set; }
}
