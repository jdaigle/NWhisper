using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MiscUtil.Conversion;
using MiscUtil.IO;

namespace NWhisper
{
    /// <remarks>
    ///  This module is an implementation of the Whisper database API
    ///  Here is the basic layout of a whisper data file
    /// 
    ///  File = Header,Data
    ///     Header = Metadata,ArchiveInfo+
    ///         Metadata = aggregationType,maxRetention,xFilesFactor,archiveCount
    ///         ArchiveInfo = Offset,SecondsPerPoint,Points
    ///     Data = Archive+
    ///         Archive = Point+
    ///             Point = timestamp,value
    /// 
    /// Struct Format (defined by http://docs.python.org/library/struct.html#format-strings)
    /// Metadata    !2LfL
    /// ArchiveInfo !3L
    /// Point       !Ld
    /// </remarks>
    public class Whipser
    {
        public static bool AutoFlush = false;
        public static bool CacheHeaders = false;

        public const int MetadataSize = 8 + 8 + 8 + 8;
        public const int ArchiveInfoSize = 8 + 8 + 8;
        public const int PointSize = 8 + 8;

        /// <remarks>
        /// Validates an archiveList.
        /// 
        /// An ArchiveList must:
        /// 1. Have at least one archive config. Example: (60, 86400)
        /// 2. No archive may be a duplicate of another.
        /// 3. Higher precision archives' precision must evenly divide all lower precision archives' precision.
        /// 4. Lower precision archives must cover larger time intervals than higher precision archives.
        /// 5. Each archive must have at least enough points to consolidate to the next archive
        /// </remars>
        public static void ValidateArchiveList(List<ArchiveInfo> archiveList)
        {
            if (archiveList == null || archiveList.Count() == 0)
            {
                throw new InvalidConfigurationException("You must specify at least one archive configuration!");
            }

            // sort by precision (secondsPerPoint)
            archiveList.Sort((x, y) => x.SecondsPerPoint.CompareTo(y.SecondsPerPoint));

            for (int i = 0; i < archiveList.Count; i++)
            {
                if (i == archiveList.Count - 1)
                {
                    break;
                }

                var archive = archiveList[i];
                var nextArchive = archiveList[i + 1];

                if (!(archive.SecondsPerPoint < nextArchive.SecondsPerPoint))
                {
                    throw new InvalidConfigurationException(
                        string.Format("A Whisper database may not configured having two archives with the same precision (archive{0}: {1}, archive{2}: {3})", i, archive.SecondsPerPoint, i + 1, nextArchive.SecondsPerPoint));
                }

                if (nextArchive.SecondsPerPoint.Mod(archive.SecondsPerPoint) != 0)
                {
                    throw new InvalidConfigurationException(
                           string.Format("Higher precision archives' precision must evenly divide all lower precision archives' precision (archive{0}: {1}, archive{2}: {3})", i, archive.SecondsPerPoint, i + 1, nextArchive.SecondsPerPoint));
                }

                if (!(nextArchive.Retention > archive.Retention))
                {
                    throw new InvalidConfigurationException(
                           string.Format("Lower precision archives must cover larger time intervals than higher precision archives (archive{0}: {1}, archive{2}: {3})", i, archive.Retention, i + 1, nextArchive.Retention));
                }

                var pointsPerConsoliation = nextArchive.SecondsPerPoint / archive.SecondsPerPoint;
                if (!(archive.Points >= pointsPerConsoliation))
                {
                    throw new InvalidConfigurationException(
                           string.Format("Each archive must have at least enough points to consolidate to the next archive (archive{0} consolidates {1} of archive{2}'s points but it has only {3} total points)", i + 1, pointsPerConsoliation, i, archive.Points));
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="path"></param>
        /// <param name="archiveList"></param>
        /// <param name="xFilesFactor">specifies the fraction of data points in a propagation interval that must have known values for a propagation to occur</param>
        /// <param name="aggregationType">the function to use when propagating data</param>
        /// <param name="sparse"></param>
        public static void Create(
            string path
            , List<ArchiveInfo> archiveList
            , double xFilesFactor = 0.5
            , AggregationType aggregationType = AggregationType.Average
            , bool sparse = false)
        {
            ValidateArchiveList(archiveList);

            if (File.Exists(path))
            {
                throw new InvalidConfigurationException(string.Format("File {0} already exists!", path));
            }

            using (var fh = File.Open(path, FileMode.Create, FileAccess.ReadWrite, FileShare.None))
            {
                try
                {
                    using (var writer = new EndianBinaryWriter(EndianBitConverter.Big, new NonClosingStreamWrapper(fh)))
                    {
                        writer.Write((long)aggregationType);
                        var maxRetention = archiveList.Max(x => x.Retention);
                        writer.Write(maxRetention);
                        writer.Write(xFilesFactor);
                        writer.Write((long)archiveList.Count);
                        var headerSize = MetadataSize + (ArchiveInfoSize * archiveList.Count);
                        long archiveOffsetPointer = headerSize;

                        foreach (var archive in archiveList) // should be sorted by ValidateArchiveList
                        {
                            writer.Write(archiveOffsetPointer);
                            writer.Write(archive.SecondsPerPoint);
                            writer.Write(archive.Points);
                            archiveOffsetPointer += archive.Points * PointSize;
                        }

                        if (sparse)
                        {
                            fh.Seek(archiveOffsetPointer - 1, SeekOrigin.Begin);
                            writer.Write((long)0);
                        }
                        else
                        {
                            var remaining = archiveOffsetPointer - headerSize;
                            var buffer = new byte[8];
                            writer.BitConverter.CopyBytes((long)0, buffer, 0);
                            // create a 16k buffer to ZERO the file
                            var chunkBuffer = new byte[16384];
                            for (int i = 0; i < chunkBuffer.Length; i += buffer.Length)
                            {
                                buffer.CopyTo(chunkBuffer, i);
                            }
                            while (remaining > chunkBuffer.Length)
                            {
                                writer.Write(chunkBuffer);
                                remaining -= chunkBuffer.Length;
                            }
                            writer.Write(chunkBuffer, 0, (int)remaining);
                        }
                    }
                    fh.Flush(AutoFlush);
                }
                finally
                {
                    fh.Close();
                }
            }
        }

        public static Header Info(string path)
        {
            using (var fh = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                try
                {
                    return ReadHeader(fh);
                }
                finally
                {
                    fh.Close();
                }
            }
        }

        private static ConcurrentDictionary<string, Header> cachedHeaders = new ConcurrentDictionary<string, Header>();

        private static Header ReadHeader(FileStream fh)
        {
            if (cachedHeaders.ContainsKey(fh.Name))
            {
                return cachedHeaders[fh.Name];
            }
            var originalOffest = fh.Position;
            fh.Seek(0, SeekOrigin.Begin);
            Header header;
            using (var reader = new EndianBinaryReader(EndianBitConverter.Big, new NonClosingStreamWrapper(fh)))
            {
                long aggregationType;
                long maxRetention;
                double xff;
                long archiveCount;
                try
                {
                    aggregationType = reader.ReadInt64();
                    maxRetention = reader.ReadInt64();
                    xff = reader.ReadDouble();
                    archiveCount = reader.ReadInt64();
                }
                catch (Exception e)
                {
                    throw new CorruptWhisperFileException("Unable to read header", fh.Name, e);
                }
                var archives = new List<ArchiveInfo>();
                for (int i = 0; i < archiveCount; i++)
                {
                    try
                    {
                        var offset = reader.ReadInt64();
                        var secondsPerPoint = reader.ReadInt64();
                        var points = reader.ReadInt64();
                        archives.Add(new ArchiveInfo(secondsPerPoint, points, offset));
                    }
                    catch (Exception e)
                    {
                        throw new CorruptWhisperFileException(string.Format("Unable to read archive{0} metadata", i), fh.Name, e);
                    }
                }
                header = new Header((AggregationType)aggregationType, maxRetention, xff, archives);
            }
            if (CacheHeaders)
            {
                cachedHeaders.TryAdd(fh.Name, header);
            }
            return header;
        }

        public static double Aggregate(AggregationType aggregationType, IEnumerable<double> knownValues)
        {
            switch (aggregationType)
            {
                case AggregationType.Average:
                    return knownValues.Average();
                case AggregationType.Sum:
                    return knownValues.Sum();
                case AggregationType.Last:
                    return knownValues.Last();
                case AggregationType.Max:
                    return knownValues.Max();
                case AggregationType.Min:
                    return knownValues.Min();
                default:
                    throw new InvalidAggregationMethodException(string.Format("Unrecognized aggregation method {0}", aggregationType));
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fromTime">epoch time</param>
        /// <param name="untilTime">epoch time</param>
        public static ArchiveFetch? Fetch(string path, long fromTime, long? untilTime = null, long? now = null)
        {
            using (var fh = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                try
                {
                    return FileFetch(fh, fromTime, untilTime, now);
                }
                finally
                {
                    fh.Close();
                }
            }
        }

        private static ArchiveFetch? FileFetch(FileStream fh, long fromTime, long? untilTime, long? now)
        {
            var header = ReadHeader(fh);
            if (!now.HasValue)
            {
                now = DateTime.UtcNow.ToUnixTime();
            }
            if (!untilTime.HasValue)
            {
                untilTime = now.Value;
            }

            // Here we try and be flexible and return as much data as we can.
            // If the range of data is from too far in the past or fully in the future, we
            // return nothing
            if (fromTime > untilTime)
            {
                throw new InvalidTimeIntervalException(string.Format("Invalid time interval: from time '{0}' is after until time '{1}'", fromTime, untilTime));
            }

            var oldestTime = (long)now - header.MaxRetention;
            // Range is in the future
            if (fromTime > now)
            {
                return null;
            }
            // Range is beyond retention
            if (untilTime < oldestTime)
            {
                return null;
            }
            // Range requested is partially beyond retention, adjust
            if (fromTime < oldestTime)
            {
                fromTime = oldestTime;
            }
            // Range is partially in the future, adjust
            if (untilTime > now)
            {
                untilTime = now;
            }

            var diff = now - fromTime;
            ArchiveInfo? archive = null;
            foreach (var testArchive in header.ArchiveList)
            {
                if (testArchive.Retention >= diff)
                {
                    archive = testArchive;
                    break;
                }
            }

            if (archive == null)
            {
                return null;
            }

            return ArchiveFetch(fh, archive.Value, fromTime, untilTime.Value);
        }

        /// <summary>
        /// Fetch data from a single archive. Note that checks for validity of the time
        /// period requested happen above this level so it's possible to wrap around the
        /// archive on a read and request data older than the archive's retention
        /// </summary>
        private static ArchiveFetch ArchiveFetch(FileStream fh, ArchiveInfo archive, long fromTime, long untilTime)
        {
            Debug.WriteLine(string.Format("ArchiveFetch from {0} to {1} in archive [{2},{3}]", fromTime, untilTime, archive.SecondsPerPoint, archive.Points));
            var fromInterval = (fromTime - (fromTime.Mod(archive.SecondsPerPoint))) + (int)archive.SecondsPerPoint;
            var untilInterval = (untilTime - (untilTime.Mod(archive.SecondsPerPoint))) + (int)archive.SecondsPerPoint;
            fh.Seek(archive.Offset, SeekOrigin.Begin);
            using (var reader = new EndianBinaryReader(EndianBitConverter.Big, new NonClosingStreamWrapper(fh)))
            {
                var baseInterval = reader.ReadInt64(); // timestamp
                var baseValue = reader.ReadDouble(); // value

                if (baseInterval == 0)
                {
                    var step = archive.SecondsPerPoint;
                    var points = (int)((untilInterval - fromInterval) / step);
                    var _timeInfo = new TimeInfo(fromInterval, untilInterval, archive.SecondsPerPoint);
                    var _valueList = Enumerable.Repeat(new PointPair(0, 0), points).ToList();
                    return new ArchiveFetch(_timeInfo, _valueList);
                }

                // Determine fromOffset
                var timeDistance = fromInterval - baseInterval;
                var pointDistance = timeDistance / archive.SecondsPerPoint;
                var byteDistance = pointDistance * PointSize;
                var fromOffset = (int)(archive.Offset + (byteDistance.Mod(archive.Size)));

                // Determine untilOffset
                timeDistance = untilInterval - baseInterval;
                pointDistance = timeDistance / archive.SecondsPerPoint;
                byteDistance = pointDistance * PointSize;
                var untilOffset = (int)(archive.Offset + (byteDistance.Mod(archive.Size)));

                // read all the points in the interval
                fh.Seek(fromOffset, SeekOrigin.Begin);
                byte[] seriesBuffer;
                int bytesRead = 0;
                if (fromOffset < untilOffset)
                {
                    // If we don't wrap around the archive
                    seriesBuffer = new byte[(int)(untilOffset - fromOffset)];
                    bytesRead += fh.Read(seriesBuffer, 0, seriesBuffer.Length);
                    if (bytesRead != seriesBuffer.Length)
                    {
                        throw new CorruptWhisperFileException(string.Format("read: {0} != {1}", bytesRead, seriesBuffer.Length));
                    }
                    Debug.WriteLine(string.Format("read {0} points starting at offset {1}", (bytesRead / PointSize), fromOffset));
                }
                else
                {
                    // We do wrap around the archive, so we need two reads
                    var archiveEnd = archive.Offset + archive.Size;

                    var firstPart = (int)(archiveEnd - fromOffset);
                    var secondPart = (int)(untilOffset - archive.Offset);

                    seriesBuffer = new byte[firstPart + secondPart];
                    bytesRead += fh.Read(seriesBuffer, 0, firstPart);
                    Debug.WriteLine(string.Format("read {0} points starting at offset {1}", (firstPart / PointSize), fromOffset));

                    fh.Seek(archive.Offset, SeekOrigin.Begin);
                    bytesRead += fh.Read(seriesBuffer, firstPart, secondPart);
                    Debug.WriteLine(string.Format("read {0} points starting at offset {1}", (secondPart / PointSize), archive.Offset));
                }

                var valueList = UnpackSeriesBuffer(seriesBuffer, bytesRead);

                var timeInfo = new TimeInfo(fromInterval, untilInterval, archive.SecondsPerPoint);
                return new ArchiveFetch(timeInfo, valueList.Where(x => !x.Equals(default(PointPair)) && x.Timestamp != default(long)).ToList());
            }
        }

        private static PointPair[] UnpackSeriesBuffer(byte[] seriesBuffer, int bytesRead)
        {
            var valueList = new PointPair[bytesRead / PointSize];
            using (var seriesMemoryStream = new MemoryStream(seriesBuffer))
            {
                using (var seriesReader = new EndianBinaryReader(EndianBitConverter.Big, seriesMemoryStream))
                {
                    for (int i = 0; i < valueList.Length; i++)
                    {
                        var timestamp = seriesReader.ReadInt64();
                        var value = seriesReader.ReadDouble();
                        valueList[i] = new PointPair(timestamp, value);
                        //Debug.WriteLine(string.Format("Reading Point ({0},{1}) from i = {2}", timestamp, value, i));
                    }
                }
            }
            return valueList;
        }

        public static void Update(string path, double value, long? timestamp = null, long? now = null)
        {
            using (var fh = File.Open(path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read))
            {
                try
                {
                    FileUpdate(fh, value, timestamp, now);
                }
                finally
                {
                    fh.Close();
                }
            }
        }

        private static void FileUpdate(FileStream fh, double value, long? timestamp, long? now)
        {
            var header = ReadHeader(fh);
            now = now ?? DateTime.UtcNow.ToUnixTime();
            if (!timestamp.HasValue)
            {
                timestamp = now.Value;
            }

            var diff = now - timestamp;
            if (!(diff < header.MaxRetention && diff >= 0))
            {
                throw new TimestampNotCoveredException("Timestamp not covered by any archives in this database.");
            }

            List<ArchiveInfo> lowerArchives = null;
            ArchiveInfo archive = new ArchiveInfo();

            for (int i = 0; i < header.ArchiveList.Count; i++)
            {
                archive = header.ArchiveList[i];
                // Find the highest-precision archive that covers timestamp
                if (archive.Retention < diff)
                {
                    continue;
                }
                // We'll pass on the update to these lower precision archives later
                lowerArchives = header.ArchiveList.Skip(i + 1).ToList();
                break;
            }

            using (var reader = new EndianBinaryReader(EndianBitConverter.Big, new NonClosingStreamWrapper(fh)))
            using (var writer = new EndianBinaryWriter(EndianBitConverter.Big, new NonClosingStreamWrapper(fh)))
            {
                // First we update the highest-precision archive
                var myInterval = timestamp.Value - (timestamp.Mod(archive.SecondsPerPoint));
                fh.Seek(archive.Offset, SeekOrigin.Begin);
                var baseInterval = reader.ReadInt64(); // timestamp
                var baseValue = reader.ReadDouble(); // value

                if (baseInterval == 0)
                {
                    // this file's first update
                    fh.Seek(archive.Offset, SeekOrigin.Begin);
                    writer.Write(myInterval);
                    writer.Write(value);
                    baseInterval = myInterval;
                    baseValue = value;
                    Debug.WriteLine(string.Format("writing point ({0},{1}) to position {2} - first update", myInterval, value, archive.Offset));
                }
                else
                {
                    // not our first update
                    var timeDistance = myInterval - baseInterval;
                    var pointDistance = timeDistance / archive.SecondsPerPoint;
                    var byteDistance = pointDistance * PointSize;
                    var myOffset = archive.Offset + (byteDistance.Mod(archive.Size));
                    Debug.WriteLine(string.Format("calculating offset int = {0} base = {1} td = {2} pd = {3} bd = {4} offset = {5}", myInterval, baseInterval, timeDistance, pointDistance, byteDistance, myOffset));
                    fh.Seek(myOffset, SeekOrigin.Begin);
                    writer.Write(myInterval);
                    writer.Write(value);
                    Debug.WriteLine(string.Format("writing point ({0},{1}) to position {2}", myInterval, value, myOffset));
                }

                // Now we propagate the update to lower-precision archives 
                var higher = archive;
                foreach (var lower in lowerArchives)
                {
                    if (!Propagate(fh, reader, writer, header, myInterval, higher, lower))
                    {
                        break;
                    }
                    higher = lower;
                }
            }

            fh.Flush(AutoFlush);
        }

        private static bool Propagate(FileStream fh, EndianBinaryReader reader, EndianBinaryWriter writer, Header header, long timestamp, ArchiveInfo higher, ArchiveInfo lower)
        {
            var aggregationType = header.AggregationType;
            var xff = header.xFilesFactor;

            var lowerIntervalStart = timestamp - timestamp.Mod(lower.SecondsPerPoint);
            var lowerIntervalEnd = lowerIntervalStart + lower.SecondsPerPoint;

            fh.Seek(higher.Offset, SeekOrigin.Begin);
            var higherBaseInterval = reader.ReadInt64(); // timestamp
            var higherBaseValue = reader.ReadDouble(); // value

            long higherFirstOffset;
            if (higherBaseInterval == 0)
            {
                higherFirstOffset = higher.Offset;
            }
            else
            {
                var timeDistance = lowerIntervalStart - higherBaseInterval;
                var pointDistance = timeDistance / higher.SecondsPerPoint;
                var byteDistance = pointDistance * PointSize;
                higherFirstOffset = higher.Offset + byteDistance.Mod(higher.Size);
            }

            var higherPoints = lower.SecondsPerPoint / higher.SecondsPerPoint;
            var higherSize = higherPoints * PointSize;
            var relativeFirstOffset = higherFirstOffset - higher.Offset;
            var relativeLastOffset = (relativeFirstOffset + higherSize).Mod(higher.Size);
            var higherLastOffset = relativeLastOffset + higher.Offset;

            fh.Seek(higherFirstOffset, SeekOrigin.Begin);
            byte[] seriesBuffer;
            int bytesRead = 0;
            if (higherFirstOffset < higherLastOffset)
            {
                seriesBuffer = new byte[(int)(higherLastOffset - higherFirstOffset)];
                // we don't wrap the archive
                bytesRead = fh.Read(seriesBuffer, 0, seriesBuffer.Length);
            }
            else
            {
                var higherEnd = higher.Offset + higher.Size;

                var firstPart = (int)(higherEnd - higherFirstOffset);
                var secondPart = (int)(higherLastOffset - higher.Offset);

                seriesBuffer = new byte[firstPart + secondPart];
                bytesRead += fh.Read(seriesBuffer, 0, firstPart);

                fh.Seek(higher.Offset, SeekOrigin.Begin);
                bytesRead += fh.Read(seriesBuffer, firstPart, secondPart);

                //var archiveEnd = higher.Offset + higher.Size;
                //seriesBuffer = new byte[(int)(archiveEnd - higherFirstOffset) + (int)(higherLastOffset - higher.Offset)];
                //// We do wrap around the archive, so we need two reads
                //bytesRead += fh.Read(seriesBuffer, 0, (int)(archiveEnd - higherFirstOffset));
                //if (higherLastOffset < higherFirstOffset)
                //{
                //    fh.Seek(higher.Offset, SeekOrigin.Begin);
                //    bytesRead += fh.Read(seriesBuffer, 0, (int)(higherLastOffset - higher.Offset));
                //}
            }

            var neighborValues = UnpackSeriesBuffer(seriesBuffer, bytesRead);

            // Propagate aggregateValue to propagate from neighborValues if we have enough known points
            var knownValues = neighborValues.Where(x => !x.Equals(default(PointPair)) && x.Timestamp != default(long)).Select(x => x.value);
            if (knownValues.Count() == 0)
            {
                return false;
            }

            var knownPercent = (double)knownValues.Count() / (double)neighborValues.Length;
            Debug.WriteLine(string.Format("Calculate Aggregate xff = {0} for {1} points", knownPercent, knownValues.Count()));
            if (knownPercent >= xff)
            {
                // we have enough data to propagte a value
                var aggregateValue = Aggregate(aggregationType, knownValues);
                fh.Seek(lower.Offset, SeekOrigin.Begin);
                var lowerBaseInterval = reader.ReadInt64(); // timestamp
                var lowerBaseValue = reader.ReadDouble(); // value
                if (lowerBaseInterval == 0)
                {
                    // First propagated update to this lower archive
                    fh.Seek(lower.Offset, SeekOrigin.Begin);
                    writer.Write(lowerIntervalStart);
                    writer.Write(aggregateValue);
                    Debug.WriteLine(string.Format("writing aggregate point ({0},{1}) to position {2} - first update", lowerIntervalStart, aggregateValue, lower.Offset));
                }
                else
                {
                    // Not our first propagated update to this lower archive
                    var timeDistance = lowerIntervalStart - lowerBaseInterval;
                    var pointDistance = timeDistance / lower.SecondsPerPoint;
                    var byteDistance = pointDistance * PointSize;
                    var lowerOffset = lower.Offset + (byteDistance.Mod(lower.Size));
                    Debug.WriteLine(string.Format("calculating aggregate offset int = {0} base = {1} td = {2} pd = {3} bd = {4} offset = {5}", lowerIntervalStart, lowerBaseInterval, timeDistance, pointDistance, byteDistance, lowerOffset));
                    fh.Seek(lowerOffset, SeekOrigin.Begin);
                    writer.Write(lowerIntervalStart);
                    writer.Write(aggregateValue);
                    Debug.WriteLine(string.Format("writing aggregate point ({0},{1}) to position {2}", lowerIntervalStart, aggregateValue, lowerOffset));
                }
                return true;
            }
            else
            {
                return false;
            }
        }

        public static void UpdateMany(string path, IEnumerable<PointPair> points, long? now = null)
        {
            if (points == null || points.Count() == 0)
            {
                return;
            }
            // order points by timestamp, newest first
            points = points.OrderByDescending(x => x.Timestamp);
            using (var fh = File.Open(path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read))
            {
                try
                {
                    //FileUpdateMany(fh, points, now);
                    // TODO: bulk update
                }
                finally
                {
                    fh.Close();
                }
            }
        }

        //private static void FileUpdateMany(FileStream fh, IEnumerable<PointPair> points, long? now)
        //{
        //    var header = ReadHeader(fh);
        //    now = now ?? DateTime.UtcNow.ToUnixTime();
        //    var archives = header.ArchiveList.GetEnumerator();
        //    archives.MoveNext();
        //    ArchiveInfo? currentArchive = archives.Current;
        //    var currentPoints = new List<PointPair>();

        //    foreach (var point in points)
        //    {
        //        var age = now - point.Timestamp;

        //        while (currentArchive.Value.Retention < age) // we can't fit any more points in this archive
        //        {
        //            if (currentPoints.Any())
        //            {
        //                // commit all the points we've found that it can fit
        //                currentPoints.Reverse();
        //                ArchiveUpdateMany(fh, header, currentArchive.Value, currentPoints);
        //                currentPoints.Clear();
        //            }
        //            if (archives.MoveNext())
        //            {
        //                currentArchive = archives.Current;
        //            }
        //            else
        //            {
        //                currentArchive = null;
        //                break;
        //            }
        //        }

        //        if (currentArchive == null)
        //        {
        //            // drop remaining points that don't fit in the database
        //            break;
        //        }

        //        currentPoints.Add(point);
        //    }

        //    if (currentArchive.HasValue && currentPoints.Any())
        //    {
        //        // don't forget to commit after we've checked all the archives
        //        currentPoints.Reverse();
        //        ArchiveUpdateMany(fh, header, currentArchive.Value, currentPoints);
        //    }

        //    fh.Flush(AutoFlush);
        //}

        //private static void ArchiveUpdateMany(FileStream fh, Header header, ArchiveInfo archive, List<PointPair> currentPoints)
        //{
        //    using (var reader = new EndianBinaryReader(EndianBitConverter.Big, new NonClosingStreamWrapper(fh)))
        //    using (var writer = new EndianBinaryWriter(EndianBitConverter.Big, new NonClosingStreamWrapper(fh)))
        //    {
        //    }
        //}
    }
}
