using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NWhisper
{
    public class InvalidAggregationMethodException : WhisperException
    {
        public InvalidAggregationMethodException() { }
        public InvalidAggregationMethodException(string message) : base(message) { }
        public InvalidAggregationMethodException(string message, Exception innerException) : base(message, innerException) { }
    }
}
