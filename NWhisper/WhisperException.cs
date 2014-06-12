using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NWhisper
{
    public class WhisperException : Exception
    {
        public WhisperException() { }
        public WhisperException(string message) : base(message) { }
        public WhisperException(string message, Exception innerException) : base(message, innerException) { }
    }
}
