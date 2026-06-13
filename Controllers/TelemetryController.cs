using Confluent.Kafka;
using IoTGateway.Models;
using Microsoft.AspNetCore.Mvc;
using ProtoBuf;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace IoTGateway.Controllers
{

    [ApiController]
    [Route("api/[controller]")]
    public class TelemetryController(IProducer<string, byte[]> producer, ILogger<TelemetryController> logger) : ControllerBase
    {
        private readonly IProducer<string, byte[]> _producer = producer;
        private readonly ILogger<TelemetryController> _logger = logger;

        private static readonly Meter Meter = new Meter("IoTGateway", "1.0.0");
        private static readonly Counter<int> RequestCounter = Meter.CreateCounter<int>("gateway_requests_total", "Total number of requests");
        private static readonly Histogram<double> RequestDuration = Meter.CreateHistogram<double>("gateway_request_duration_ms", "Request duration in milliseconds");

        [HttpPost]
        public async Task<IActionResult> PostTelemetry([FromBody] TelemetryRequest request)
        {
            var stopwatch = Stopwatch.StartNew();

            if (request.DeviceId <= 0)
            {
                _logger.LogWarning("Invalid device id: {DeviceId}", request.DeviceId);
                return BadRequest("DeviceId must be positive");
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

                stopwatch.Stop();
                RequestCounter.Add(1, new KeyValuePair<string, object?>("device_id", request.DeviceId));
                RequestDuration.Record(stopwatch.Elapsed.TotalMilliseconds);

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
