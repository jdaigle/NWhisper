using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NWhisper
{
    public class CorruptWhisperFileException : WhisperException
    {
        public CorruptWhisperFileException(string message) : base(message) { }
        public CorruptWhisperFileException(string message, string filePath)
            : base(message)
        {
            this.FilePath = filePath;
        }
        public CorruptWhisperFileException(string message, Exception innerException) : base(message, innerException) { }
        public CorruptWhisperFileException(string message, string filePath, Exception innerException)
            : base(message, innerException)
        {
            this.FilePath = filePath;
        }

        public string FilePath { get; set; }
    }
}
