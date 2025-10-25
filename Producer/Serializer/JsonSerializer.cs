using System.Runtime.Serialization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Confluent.Kafka;

namespace Producer.Serializer
{
    public class JsonSerializer<T> : ISerializer<T>
    {
        private readonly JsonSerializerOptions _options = new()
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            Converters = { new JsonStringEnumConverter() }
        };

        public byte[] Serialize(T data, SerializationContext context)
        {
            if (data == null)
            {
                return null;
            }

            try
            {
                var json = JsonSerializer.Serialize(data, _options);

                return Encoding.UTF8.GetBytes(json);
            }
            catch (Exception ex)
            {
                throw new SerializationException($"Failed to serialize {typeof(T).Name}: {ex.Message}", ex);
            }
        }
    }
}
