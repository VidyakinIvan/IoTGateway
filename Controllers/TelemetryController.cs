using Confluent.Kafka;
using IoTGateway.Models;
using Microsoft.AspNetCore.Mvc;
using ProtoBuf;

namespace IoTGateway.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class TelemetryController(IProducer<string, byte[]> producer, 
        ILogger<TelemetryController> logger) : ControllerBase
    {
        private readonly IProducer<string, byte[]> _producer = producer;
        private readonly ILogger<TelemetryController> _logger = logger;

        [HttpPost]
        public async Task<IActionResult> PostTelemetry([FromBody] TelemetryRequest request)
        {
            if (request.DeviceId <= 0)
            {
                _logger.LogWarning("Invalid device id: {DeviceId}", request.DeviceId);
                return BadRequest("DeviceId must be positive");
            }

            if (request.Value is < -50 or > 150)
            {
                _logger.LogWarning("Invalid value: {Value} from device {DeviceId}", request.Value, request.DeviceId);
                return BadRequest("Value out of range [-50, 150]");
            }

            if (request.Timestamp <= 0)
            {
                _logger.LogWarning("Invalid timestamp from device {DeviceId}", request.DeviceId);
                return BadRequest("Timestamp must be positive");
            }

            var protobuf = new TelemetryProtobuf
            {
                DeviceId = request.DeviceId,
                TimestampMs = request.Timestamp,
                Value = request.Value,
                ReceivedAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            };

            using var ms = new MemoryStream();
            Serializer.Serialize(ms, protobuf);
            var bytes = ms.ToArray();

            try
            {
                await _producer.ProduceAsync("raw.telemetry", new Message<string, byte[]>
                {
                    Key = request.DeviceId.ToString(),
                    Value = bytes
                });
                _logger.LogInformation("Published telemetry from device {DeviceId}", request.DeviceId);
                return Ok();
            }
            catch (ProduceException<string, byte[]> ex)
            {
                _logger.LogError(ex, "Failed to publish message from device {DeviceId}", request.DeviceId);
                return StatusCode(503, "Kafka unavailable");
            }
        }
    }
}
