using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NWhisper
{
    public struct PointPair
    {
        public PointPair(long timestamp, double value)
        {
            this.Timestamp = timestamp;
            this.value = value;
        }

        public readonly long Timestamp;
        public readonly double value;

        public DateTime DateTime { get { return Timestamp.FromUnixTime(); } }
    }
}
