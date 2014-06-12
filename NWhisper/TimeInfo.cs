using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NWhisper
{
    public struct TimeInfo
    {
        public TimeInfo(long fromInterval, long untilInterval, long step)
        {
            this.FromInterval = fromInterval;
            this.UntilInterval = untilInterval;
            this.Step = step;
        }

        public readonly long Step;
        public readonly long UntilInterval;
        public readonly long FromInterval;
    }
}
