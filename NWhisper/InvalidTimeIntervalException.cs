using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NWhisper
{
    public class InvalidTimeIntervalException : WhisperException
    {
        public InvalidTimeIntervalException() { }
        public InvalidTimeIntervalException(string message) : base(message) { }
        public InvalidTimeIntervalException(string message, Exception innerException) : base(message, innerException) { }
    }
}
