using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;

namespace NWhisper.Tests
{
    [TestFixture]
    public class TestWhisper
    {
        public static string db = @"c:\temp\db.wsp";

        private long now = DateTime.UtcNow.ToUnixTime();

        [SetUp]
        public void Setup()
        {
            RemoveDb();
        }

        private static void RemoveDb()
        {
            try
            {
                if (File.Exists(db))
                {
                    File.Delete(db);
                }
            }
            catch (Exception)
            {
                // ignore
            }
        }

        [Test]
        public void validate_archive_list()
        {
            Assert.Throws<InvalidConfigurationException>(() => Whipser.ValidateArchiveList(new List<ArchiveInfo>()));
        }

        [Test]
        public void duplicates()
        {
            Assert.DoesNotThrow(() => Whipser.ValidateArchiveList(new List<ArchiveInfo>() { new ArchiveInfo(1, 60), new ArchiveInfo(60, 60) }));
            Assert.Throws<InvalidConfigurationException>(() => Whipser.ValidateArchiveList(new List<ArchiveInfo>() { new ArchiveInfo(1, 60), new ArchiveInfo(60, 60), new ArchiveInfo(1, 60) }));
        }

        [Test]
        public void even_precision_division()
        {
            Assert.DoesNotThrow(() => Whipser.ValidateArchiveList(new List<ArchiveInfo>() { new ArchiveInfo(1, 60), new ArchiveInfo(6, 60) }));
            Assert.Throws<InvalidConfigurationException>(() => Whipser.ValidateArchiveList(new List<ArchiveInfo>() { new ArchiveInfo(60, 60), new ArchiveInfo(7, 60) }));
        }

        [Test]
        public void timespan_coverage()
        {
            Assert.DoesNotThrow(() => Whipser.ValidateArchiveList(new List<ArchiveInfo>() { new ArchiveInfo(1, 60), new ArchiveInfo(60, 60) }));
            Assert.Throws<InvalidConfigurationException>(() => Whipser.ValidateArchiveList(new List<ArchiveInfo>() { new ArchiveInfo(1, 60), new ArchiveInfo(10, 1) }));
        }

        [Test]
        public void number_of_points()
        {
            Assert.DoesNotThrow(() => Whipser.ValidateArchiveList(new List<ArchiveInfo>() { new ArchiveInfo(1, 60), new ArchiveInfo(60, 60) }));
            Assert.Throws<InvalidConfigurationException>(() => Whipser.ValidateArchiveList(new List<ArchiveInfo>() { new ArchiveInfo(1, 30), new ArchiveInfo(60, 60) }));
        }

        [Test]
        public void aggregate()
        {
            Assert.AreEqual(1, Whipser.Aggregate(AggregationType.Min, new double[] { 1, 2, 3, 4 }));
            Assert.AreEqual(4, Whipser.Aggregate(AggregationType.Max, new double[] { 1, 2, 3, 4 }));
            Assert.AreEqual(4, Whipser.Aggregate(AggregationType.Last, new double[] { 3, 2, 5, 4 }));
            Assert.AreEqual(19, Whipser.Aggregate(AggregationType.Sum, new double[] { 10, 2, 3, 4 }));
            Assert.AreEqual(2.5, Whipser.Aggregate(AggregationType.Average, new double[] { 1, 2, 3, 4 }));
        }

        [Test]
        public void create()
        {
            var retention = new List<ArchiveInfo>() { new ArchiveInfo(1, 60), new ArchiveInfo(60, 60) };

            // check if invalid configuration fails successfully
            Assert.Throws<InvalidConfigurationException>(() => Whipser.Create(db, new List<ArchiveInfo>()));

            // create a new db with a valid configuration
            Whipser.Create(db, retention);

            // attempt to create another db in the same file, this should fail
            Assert.Throws<InvalidConfigurationException>(() => Whipser.Create(db, retention));

            var info = Whipser.Info(db);

            Assert.AreEqual(retention.Max(x => x.Retention), info.MaxRetention);
            Assert.AreEqual(AggregationType.Average, info.AggregationType);
            Assert.AreEqual(0.5f, info.xFilesFactor);

            Assert.AreEqual(retention.Count, info.ArchiveList.Count);
            Assert.AreEqual(retention[0].SecondsPerPoint, info.ArchiveList[0].SecondsPerPoint);
            Assert.AreEqual(retention[0].Points, info.ArchiveList[0].Points);
            Assert.AreEqual(retention[1].SecondsPerPoint, info.ArchiveList[1].SecondsPerPoint);
            Assert.AreEqual(retention[1].Points, info.ArchiveList[1].Points);

            RemoveDb();
        }

        [Test]
        public void fetch()
        {
            Assert.Throws<FileNotFoundException>(() => Whipser.Fetch("does_not_exist", 0));

            var retention = new List<ArchiveInfo>() { new ArchiveInfo(1, 60), new ArchiveInfo(60, 60), new ArchiveInfo(3600, 24), new ArchiveInfo(86400, 365) };
            Whipser.Create(db, retention);

            Assert.Throws<InvalidTimeIntervalException>(() => Whipser.Fetch(db, DateTime.Now.Ticks, DateTime.Now.Ticks - 60000));

            var fetch = Whipser.Fetch(db, 0).Value;

            // check time range
            Assert.AreEqual(retention.Last().Retention, fetch.TimeInfo.UntilInterval - fetch.TimeInfo.FromInterval);

            // check number of points
            Assert.AreEqual(retention.Last().Points, fetch.ValueList.Count);

            // check step size
            Assert.AreEqual(retention.Last().SecondsPerPoint, fetch.TimeInfo.Step);

            RemoveDb();
        }

        private static Random random = new Random();

        private PointPair[] update(string wsp = null, List<ArchiveInfo> schema = null)
        {
            wsp = wsp ?? db;
            schema = schema ?? new List<ArchiveInfo>() { new ArchiveInfo(1, 20) };

            var numDataPoints = schema[0].Points;

            Whipser.Create(wsp, schema);

            var tn = DateTime.UtcNow.ToUnixTime() - numDataPoints;
            var data = new PointPair[numDataPoints];
            for (int i = 0; i < numDataPoints; i++)
            {
                //data[i] = new PointPair(tn + 1 + i, i * 10);
                data[i] = new PointPair(tn + 1 + i, random.Next(1000) * 10);
            }

            //Whipser.Update(wsp, data[0].value, data[0].Timestamp);
            //Whipser.UpdateMany(wsp, data);
            foreach (var item in data)
            {
                Whipser.Update(wsp, item.value, item.Timestamp);
            }

            // add more fake data
            Thread.Sleep(1000);
            Whipser.Update(wsp, random.Next(1000) * 10);
            Thread.Sleep(1000);
            Whipser.Update(wsp, random.Next(1000) * 10);

            return data;
        }

        [Test]
        public void update_single_archive()
        {
            var retention_schema = new List<ArchiveInfo>() { new ArchiveInfo(1, 20) };
            var data = update(schema: retention_schema);

            var fetch = Whipser.Fetch(db, 0).Value;
            var fetchedData = fetch.ValueList;

            for (int i = 0; i < data.Length; i++)
            {
                Assert.AreEqual(data[i].value, fetchedData[i].value);
            }

            // in future
            Assert.Throws<TimestampNotCoveredException>(() => Whipser.Update(db, 1.337, now + 1, now));

            // before the past
            Assert.Throws<TimestampNotCoveredException>(() => Whipser.Update(db, 1.337, now - retention_schema[0].Retention - 1, now));

            RemoveDb();
        }

        [Test]
        public void update_multi_archive()
        {
            var retention_schema = new List<ArchiveInfo>() { new ArchiveInfo(1, 60), new ArchiveInfo(60, 60) };
            var data = update(schema: retention_schema);

            var fetch = Whipser.Fetch(db, DateTime.UtcNow.ToUnixTime() - 25, now: DateTime.UtcNow.ToUnixTime()+5).Value;
            var fetchedData = fetch.ValueList;

            foreach (var item in data)
            {
                Debug.WriteLine("wrote point ({0},{1})", item.Timestamp, item.value);
            }
            foreach (var item in fetchedData)
            {
                Debug.WriteLine("read point ({0},{1})", item.Timestamp, item.value);
            }

            // in future
            Assert.Throws<TimestampNotCoveredException>(() => Whipser.Update(db, 1.337, DateTime.UtcNow.ToUnixTime() + 1, now));

            // before the past
            Assert.Throws<TimestampNotCoveredException>(() => Whipser.Update(db, 1.337, DateTime.UtcNow.ToUnixTime() - retention_schema[1].Retention - 1, now));

            RemoveDb();
        }

        [Test]
        public void calc_database_size()
        {
            // create a new db with a valid configuration
            var retention = new List<ArchiveInfo>() { new ArchiveInfo(1, 60), new ArchiveInfo(60, 60), new ArchiveInfo(3600, 24), new ArchiveInfo(86400, 365) };
            Whipser.Create(db, retention);
            Debug.WriteLine("File Size = ", new FileInfo(db).Length);
            RemoveDb();
        }
    }
}
