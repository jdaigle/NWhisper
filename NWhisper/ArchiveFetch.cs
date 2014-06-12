using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NWhisper
{
    public struct ArchiveFetch
    {
        public ArchiveFetch(TimeInfo timeInfo, List<PointPair> valueList)
        {
            this.TimeInfo = timeInfo;
            this.ValueList = valueList;
        }

        public readonly TimeInfo TimeInfo;
        public readonly List<PointPair> ValueList;
    }
}
