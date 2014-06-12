using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NWhisper
{
    public class TimestampNotCoveredException : WhisperException
    {
        public TimestampNotCoveredException() { }
        public TimestampNotCoveredException(string message) : base(message) { }
        public TimestampNotCoveredException(string message, Exception innerException) : base(message, innerException) { }
    }
}
