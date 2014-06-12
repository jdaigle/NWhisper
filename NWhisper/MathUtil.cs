using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NWhisper
{
    public static class MathUtil
    {
        public static int Mod(this int a, int b)
        {
            return (int)Mod((long)a, (long)b);
        }

        public static long Mod(this long a, long b)
        {
            var r = a % b;
            if (r < 0)
            {
                r += b;
            }
            return r;
        }

        public static long Mod(this long? a, long? b)
        {
            return Mod(a.Value, b.Value);
        }
    }
}
