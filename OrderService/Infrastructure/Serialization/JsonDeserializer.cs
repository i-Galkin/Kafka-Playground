using System.Runtime.Serialization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Confluent.Kafka;

namespace OrderService.Infrastructure.Serialization
{
    public class JsonDeserializer<T> : IDeserializer<T>
    {
        private readonly JsonSerializerOptions _options = new()
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            Converters = { new JsonStringEnumConverter() },
            PropertyNameCaseInsensitive = true,
        };

        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull || data.Length == 0)
            {
                return default;
            }

            try
            {
                var json = Encoding.UTF8.GetString(data);
                var result = JsonSerializer.Deserialize<T>(json, _options);

                return result;
            }
            catch (JsonException ex)
            {
                throw new SerializationException($"Failed to deserialize {typeof(T).Name}: {ex.Message}. Data: {Encoding.UTF8.GetString(data)}", ex);
            }
        }
    }
}
