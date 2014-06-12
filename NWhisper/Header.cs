using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NWhisper
{
    public struct Header
    {
        public Header(AggregationType aggregationType, long maxRetention, double xFilesFactor, List<ArchiveInfo> archiveList)
        {
            this.AggregationType = aggregationType;
            this.MaxRetention = maxRetention;
            this.xFilesFactor = xFilesFactor;
            this.ArchiveList = archiveList;
        }

        public readonly AggregationType AggregationType;
        public readonly long MaxRetention;
        public readonly double xFilesFactor;
        public readonly List<ArchiveInfo> ArchiveList;
    }
}
