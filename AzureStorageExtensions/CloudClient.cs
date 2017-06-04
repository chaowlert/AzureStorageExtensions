using System;
using System.Collections.Concurrent;
using System.Configuration;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureStorageExtensions
{
    public class CloudClient
    {
        readonly ConcurrentDictionary<string, Tuple<DateTime, object>> _references = new ConcurrentDictionary<string, Tuple<DateTime, object>>();

        static readonly ConcurrentDictionary<string, CloudClient> _dict = new ConcurrentDictionary<string, CloudClient>();
        public static CloudClient Get(string key)
        {
            var connectionString = ConfigurationManager.ConnectionStrings[key].ConnectionString;
            return _dict.GetOrAdd(connectionString, k => new CloudClient(k));
        }

        public CloudClient(string connectionString)
        {
            // Create clients
            Account = CloudStorageAccount.Parse(connectionString);
            Table = Account.CreateCloudTableClient();
            Blob = Account.CreateCloudBlobClient();
            Queue = Account.CreateCloudQueueClient();
        }

        public CloudStorageAccount Account { get; }

        public CloudTableClient Table { get; }

        public CloudBlobClient Blob { get; }

        public CloudQueueClient Queue { get; }

        T GetCloudObject<T>(string key, SettingAttribute setting, Func<string, T> getReference, Action<T, SettingAttribute> createIfNotExists, Action<T> deleteIfExists)
        {
            if (_references.TryGetValue(key, out var tuple))
            {
                if (setting.Period == Period.NoPeriod || DateTime.UtcNow < tuple.Item1)
                    return (T)tuple.Item2;
            }

            var now = DateTime.UtcNow;
            var objName = GetName(setting, now);
            var obj = getReference(objName);
            var expire = GetExpireDate(setting, now);
            _references[key] = Tuple.Create(expire, (object)obj);

            //maintain
            createIfNotExists(obj, setting);
            if (setting.Period != Period.NoPeriod && setting.RemoveAfter > 0)
            {
                var maxDelete = setting.Period == Period.Day ? 7 : 1;
                for (var i = 0; i < maxDelete; i++)
                {
                    var previous = GetPreviousDate(setting, now.AddDays(-i));
                    var objNameToDelete = GetName(setting, previous);
                    var objToDelete = getReference(objNameToDelete);
                    deleteIfExists(objToDelete);
                }
            }

            return obj;
        }

        public CloudTable GetCloudTable(string key, SettingAttribute setting)
        {
            return GetCloudObject(key, setting, Table.GetTableReference, (t, s) => t.CreateIfNotExists(), t => t.DeleteIfExists());
        }
        public CloudQueue GetCloudQueue(string key, SettingAttribute setting)
        {
            return GetCloudObject(key, setting, Queue.GetQueueReference, (q, s) => q.CreateIfNotExists(), q => q.DeleteIfExists());
        }
        public CloudBlobContainer GetCloudBlobContainer(string key, SettingAttribute setting)
        {
            return GetCloudObject(key, setting, Blob.GetContainerReference, (b, s) => b.CreateIfNotExists(s.BlobAccessType), b => b.DeleteIfExists());
        }
        public CloudTable<T> GetGenericCloudTable<T>(string key, SettingAttribute setting) where T : class, ITableEntity, new()
        {
            return GetCloudObject(key, setting, CreateCloudTable<T>, (t, s) => t.CloudTableContext.CreateIfNotExists(), t => t.CloudTableContext.DeleteIfExists());
        }
        CloudTable<T> CreateCloudTable<T>(string tableName) where T : class, ITableEntity, new()
        {
            var table = Table.GetTableReference(tableName);
            return new CloudTable<T>(table);
        }

        static string GetName(SettingAttribute setting, DateTime date)
        {
            switch (setting.Period)
            {
                case Period.NoPeriod:
                    return setting.Name;
                case Period.Year:
                    return setting.Name + date.ToString("yyyy");
                case Period.Month:
                    return setting.Name + date.ToString("yyyyMM");
                case Period.Day:
                    return setting.Name + date.ToString("yyyyMMdd");
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        static DateTime GetExpireDate(SettingAttribute setting, DateTime date)
        {
            switch (setting.Period)
            {
                case Period.NoPeriod:
                    return DateTime.MaxValue;
                case Period.Year:
                    return new DateTime(date.Year + 1, 1, 1);
                case Period.Month:
                    return new DateTime(date.Year, date.Month, 1).AddMonths(1);
                case Period.Day:
                    return date.Date.AddDays(1);
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        static DateTime GetPreviousDate(SettingAttribute setting, DateTime date)
        {
            switch (setting.Period)
            {
                case Period.NoPeriod:
                    return date;
                case Period.Year:
                    return date.AddYears(-setting.RemoveAfter);
                case Period.Month:
                    return date.AddMonths(-setting.RemoveAfter);
                case Period.Day:
                    return date.AddDays(-setting.RemoveAfter);
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
    }
}
