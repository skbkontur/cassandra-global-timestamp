using System.Globalization;
using System.Text;

using JetBrains.Annotations;

namespace SkbKontur.Cassandra.GlobalTimestamp
{
    internal static class Serializer
    {
        public static long Deserialize([NotNull] byte[] bytes)
        {
            return long.Parse(Encoding.UTF8.GetString(bytes), CultureInfo.InvariantCulture);
        }

        [NotNull]
        public static byte[] Serialize(long value)
        {
            return Encoding.UTF8.GetBytes(value.ToString(CultureInfo.InvariantCulture));
        }
    }
}