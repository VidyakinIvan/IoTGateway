using System.Text.Json.Serialization;

namespace IoTGateway.Models
{
    public class TelemetryRequest
    {
        [JsonPropertyName("deviceId")]
        public int DeviceId { get; set; }

        [JsonPropertyName("timestamp")]
        public long Timestamp { get; set; }

        [JsonPropertyName("value")]
        public double Value { get; set; }
    }
}
