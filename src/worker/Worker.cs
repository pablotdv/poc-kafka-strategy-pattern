using common;
using Confluent.Kafka;
using Newtonsoft.Json;

namespace worker;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IConsumer<Null, string> _consumer;
    private readonly List<IEventStrategy> _eventStrategies;

    public Worker(ILogger<Worker> logger)
    {
        _logger = logger;
        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "poc.consumer",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        _consumer = new ConsumerBuilder<Null, string>(config).Build();
        _consumer.Subscribe("poc.topic");

        _eventStrategies = new List<IEventStrategy>()
        {
            new MessageEventStrategy(),
            new CallbackMessageEventStrategy(),
            new CallbackStatusEventStrategy(),
            new TransbordoEventStrategy()
        };

    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
            var consumeResult = _consumer.Consume(stoppingToken);
            // deserialize consumeResult into a MessageEvent
            var jsonSerializerSettings = new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.All
            };

           var baseEvent = JsonConvert.DeserializeObject<BaseEvent>(consumeResult.Message.Value, jsonSerializerSettings);

            var eventStrategy = _eventStrategies.FirstOrDefault(es => es.CanHandle(baseEvent));

            if (eventStrategy != null)
            {
                eventStrategy.Handle(baseEvent);
            }
            else
            {
                Console.WriteLine("No strategy found");
            }

            await Task.CompletedTask;
        }
    }
}

public interface IEventStrategy
{
    bool CanHandle(BaseEvent baseEvent);
    void Handle(BaseEvent baseEvent);
}


public class MessageEventStrategy : IEventStrategy
{
    public bool CanHandle(BaseEvent baseEvent)
    {
        return baseEvent is MessageEvent;
    }

    public void Handle(BaseEvent baseEvent)
    {
        var message = baseEvent as MessageEvent;

        Console.WriteLine("MessageEvent");
        Console.WriteLine($"Id: {message.Id}");
        Console.WriteLine($"Message: {message.Message}");
        Console.WriteLine($"Timestamp: {message.Timestamp}");
    }
}

public class CallbackMessageEventStrategy : IEventStrategy
{
    public bool CanHandle(BaseEvent baseEvent)
    {
        return baseEvent is CallbackMessageEvent;
    }

    public void Handle(BaseEvent baseEvent)
    {
        var callbackMessage = baseEvent as CallbackMessageEvent;

        Console.WriteLine("CallbackMessageEvent");
        Console.WriteLine($"Id: {callbackMessage.Id}");
        Console.WriteLine($"CallbackMessage: {callbackMessage.CallbackMessage}");
        Console.WriteLine($"Timestamp: {callbackMessage.Timestamp}");
    }
}

public class CallbackStatusEventStrategy : IEventStrategy
{
    public bool CanHandle(BaseEvent baseEvent)
    {
        return baseEvent is CallbackStatusEvent;
    }

    public void Handle(BaseEvent baseEvent)
    {
        var callbackStatus = baseEvent as CallbackStatusEvent;

        Console.WriteLine("CallbackStatusEvent");
        Console.WriteLine($"Id: {callbackStatus.Id}");
        Console.WriteLine($"CallbackStatus: {callbackStatus.CallbackStatus}");
        Console.WriteLine($"Timestamp: {callbackStatus.Timestamp}");
    }
}

public class TransbordoEventStrategy : IEventStrategy
{
    public bool CanHandle(BaseEvent baseEvent)
    {
        return baseEvent is TransbordoEvent;
    }

    public void Handle(BaseEvent baseEvent)
    {
        var transbordo = baseEvent as TransbordoEvent;

        Console.WriteLine("TransbordoEvent");
        Console.WriteLine($"Id: {transbordo.Id}");
        Console.WriteLine($"Transbordo: {transbordo.Transbordo}");
        Console.WriteLine($"Timestamp: {transbordo.Timestamp}");
    }
}
