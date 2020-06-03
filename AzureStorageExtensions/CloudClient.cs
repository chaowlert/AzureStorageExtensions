using System;
using System.Collections.Concurrent;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage.Queue;

namespace AzureStorageExtensions
{
    public class CloudClient
    {
        readonly ConcurrentDictionary<string, Tuple<DateTime, object>> _references = new ConcurrentDictionary<string, Tuple<DateTime, object>>();

        static readonly ConcurrentDictionary<string, CloudClient> _dict = new ConcurrentDictionary<string, CloudClient>();
        public static CloudClient Get(string connectionString)
        {
            return _dict.GetOrAdd(connectionString, k => new CloudClient(k));
        }

        public CloudClient(string connectionString)
        {
            // Create clients
            TableAccount = CloudStorageAccount.Parse(connectionString);
            StorageAccount = Microsoft.Azure.Storage.CloudStorageAccount.Parse(connectionString);
            Table = TableAccount.CreateCloudTableClient();
            Blob = StorageAccount.CreateCloudBlobClient();
            Queue = StorageAccount.CreateCloudQueueClient();
        }

        public CloudStorageAccount TableAccount { get; }

        public Microsoft.Azure.Storage.CloudStorageAccount StorageAccount { get; }

        public CloudTableClient Table { get; }

        public CloudBlobClient Blob { get; }

        public CloudQueueClient Queue { get; }

        T GetCloudObject<T>(SettingAttribute setting, bool autoCreate, Func<string, T> getReference, Action<T, SettingAttribute> createIfNotExists, Action<T> deleteIfExists)
        {
            if (setting.Period == Period.NoPeriod)
            {
                var @ref = getReference(setting.Name);
                if (autoCreate)
                    createIfNotExists(@ref, setting);
                return @ref;
            }

            var key = typeof(T).FullName + "." + setting.Name;
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
            if (setting.RemoveAfter > 0)
            {
                var previous = GetPreviousDate(setting, now);
                var objNameToDelete = GetName(setting, previous);
                var objToDelete = getReference(objNameToDelete);
                deleteIfExists(objToDelete);
            }

            return obj;
        }

        public CloudTable GetCloudTable(SettingAttribute setting, bool autoCreate)
        {
            return GetCloudObject(setting, autoCreate, Table.GetTableReference, (t, s) => t.CreateIfNotExists(), t => t.DeleteIfExists());
        }
        public CloudQueue GetCloudQueue(SettingAttribute setting, bool autoCreate)
        {
            return GetCloudObject(setting, autoCreate, Queue.GetQueueReference, (q, s) => q.CreateIfNotExists(), q => q.DeleteIfExists());
        }
        public CloudBlobContainer GetCloudBlobContainer(SettingAttribute setting, bool autoCreate)
        {
            return GetCloudObject(setting, autoCreate, Blob.GetContainerReference, (b, s) => b.CreateIfNotExists(s.BlobAccessType), b => b.DeleteIfExists());
        }
        public CloudTable<T> GetGenericCloudTable<T>(SettingAttribute setting, bool autoCreate) where T : class, ITableEntity, new()
        {
            return GetCloudObject(setting, autoCreate, CreateCloudTable<T>, (t, s) => t.CloudTableContext.CreateIfNotExists(), t => t.CloudTableContext.DeleteIfExists());
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
                case Period.Year:
                    return setting.Name + date.ToString("yyyy");
                case Period.Month:
                    return setting.Name + date.ToString("yyyyMM");
                case Period.Day:
                    return setting.Name + date.ToString("yyyyMMdd");
                default:
                    return setting.Name;
            }
        }

        static DateTime GetExpireDate(SettingAttribute setting, DateTime date)
        {
            switch (setting.Period)
            {
                case Period.Year:
                    return new DateTime(date.Year + 1, 1, 1);
                case Period.Month:
                    return new DateTime(date.Year, date.Month, 1).AddMonths(1);
                case Period.Day:
                    return date.Date.AddDays(1);
                default:
                    return DateTime.MaxValue;
            }
        }

        static DateTime GetPreviousDate(SettingAttribute setting, DateTime date)
        {
            switch (setting.Period)
            {
                case Period.Year:
                    return date.AddYears(-setting.RemoveAfter);
                case Period.Month:
                    return date.AddMonths(-setting.RemoveAfter);
                case Period.Day:
                    return date.AddDays(-setting.RemoveAfter);
                default:
                    return date;
            }
        }
    }
}
