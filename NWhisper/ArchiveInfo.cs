using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NWhisper
{
    public struct ArchiveInfo
    {
        public ArchiveInfo(long secondsPerPoint, long points)
        {
            this.SecondsPerPoint = secondsPerPoint;
            this.Points = points;
            this.Offset = -1;
        }

        public ArchiveInfo(long secondsPerPoint, long points, long offset)
        {
            this.SecondsPerPoint = secondsPerPoint;
            this.Points = points;
            this.Offset = offset;
        }

        public readonly long SecondsPerPoint;
        public readonly long Points;

        public readonly long Offset;

        public long Retention { get { return SecondsPerPoint * Points; } }
        public long Size { get { return Whipser.PointSize * Points; } }
    }
}
