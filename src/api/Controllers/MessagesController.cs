using common;
using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace api.Controllers;

[ApiController]
[Route("[controller]")]
public class MessagesController : ControllerBase
{
    private readonly IProducer<Null, String> _producer;

    public MessagesController(IProducer<Null, string> producer)
    {
        _producer = producer;
    }

    [HttpPost]
    [Route("message")]
    public async Task<IActionResult> PostMessage([FromBody] string message)
    {
        var messageEvent = new MessageEvent()
        {
            Id = Guid.NewGuid(),
            Message = message,
            Timestamp = DateTime.Now
        };
        
        var jsonSerializerSettings = new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.All
        };

        var jsonSerializer = JsonConvert.SerializeObject(messageEvent, jsonSerializerSettings);


        var result = await _producer.ProduceAsync("poc.topic", new Message<Null, string> { Value = jsonSerializer });
        return Ok(result);
    }

    [HttpPost]
    [Route("callbackmessages")]
    public async Task<IActionResult> PostCallbackMessages([FromBody] string message)
    {
        var callbackMessageEvent = new CallbackMessageEvent()
        {
            Id = Guid.NewGuid(),
            CallbackMessage = message,
            Timestamp = DateTime.Now
        };
        
        var jsonSerializerSettings = new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.All
        };

        var jsonSerializer = JsonConvert.SerializeObject(callbackMessageEvent, jsonSerializerSettings);


        var result = await _producer.ProduceAsync("poc.topic", new Message<Null, string> { Value = jsonSerializer });
        return Ok(result);
    }

    [HttpPost]
    [Route("callbackstatus")]
    public async Task<IActionResult> PostCallbackStatus([FromBody] string message)
    {
        var callbackStatusEvent = new CallbackStatusEvent()
        {
            Id = Guid.NewGuid(),
            CallbackStatus = message,
            Timestamp = DateTime.Now
        };
        
        var jsonSerializerSettings = new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.All
        };

        var jsonSerializer = JsonConvert.SerializeObject(callbackStatusEvent, jsonSerializerSettings);

        var result = await _producer.ProduceAsync("poc.topic", new Message<Null, string> { Value = jsonSerializer });
        return Ok(result);
    }

    [HttpPost]
    [Route("transbordo")]
    public async Task<IActionResult> PostTransbordo([FromBody] string message)
    {
        var transbordoEvent = new TransbordoEvent()
        {
            Id = Guid.NewGuid(),
            Transbordo = message,
            Timestamp = DateTime.Now
        };
        
        var jsonSerializerSettings = new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.All
        };

        var jsonSerializer = JsonConvert.SerializeObject(transbordoEvent, jsonSerializerSettings);

        var result = await _producer.ProduceAsync("poc.topic", new Message<Null, string> { Value = jsonSerializer });
        return Ok(result);
    }
}