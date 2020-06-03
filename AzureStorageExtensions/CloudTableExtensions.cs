using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table;

namespace AzureStorageExtensions
{
    public static class CloudTableExtensions
    {
        public static bool Lease<T>(this CloudTable<T> table, string partitionKey, string rowKey, TimeSpan leaseTime) where T : class, ITableEntity, ILeasable, new()
        {
            var entity = table.Retrieve(partitionKey, rowKey);
            if (entity != null)
                return table.Lease(entity, leaseTime);

            try
            {
                table.Insert(new T
                {
                    PartitionKey = partitionKey,
                    RowKey = rowKey,
                    LeaseExpire = DateTime.UtcNow.Add(leaseTime),
                });
                return true;
            }
            catch (StorageException e)
            {
                if (e.RequestInformation.HttpStatusCode != (int)HttpStatusCode.PreconditionFailed &&
                    e.RequestInformation.HttpStatusCode != (int)HttpStatusCode.Conflict)
                    throw;
                return false;
            }
        }
        public static async Task<bool> LeaseAsync<T>(this CloudTable<T> table, string partitionKey, string rowKey, TimeSpan leaseTime) where T : class, ITableEntity, ILeasable, new()
        {
            var entity = await table.RetrieveAsync(partitionKey, rowKey);
            if (entity != null)
                return await table.LeaseAsync(entity, leaseTime);

            try
            {
                await table.InsertAsync(new T
                {
                    PartitionKey = partitionKey,
                    RowKey = rowKey,
                    LeaseExpire = DateTime.UtcNow.Add(leaseTime),
                });
                return true;
            }
            catch (StorageException e)
            {
                if (e.RequestInformation.HttpStatusCode != (int)HttpStatusCode.PreconditionFailed &&
                    e.RequestInformation.HttpStatusCode != (int)HttpStatusCode.Conflict)
                    throw;
                return false;
            }
        }

        public static bool Lease<T>(this CloudTable<T> table, T entity, TimeSpan leaseTime) where T : class, ITableEntity, ILeasable, new()
        {
            var now = DateTime.UtcNow;
            if (entity.LeaseExpire.HasValue && entity.LeaseExpire.Value >= now)
                return false;
            entity.LeaseExpire = now.Add(leaseTime);

            try
            {
                table.Update(entity);
                return true;
            }
            catch (StorageException e)
            {
                if (e.RequestInformation.HttpStatusCode != (int)HttpStatusCode.PreconditionFailed &&
                    e.RequestInformation.HttpStatusCode != (int)HttpStatusCode.Conflict &&
                    e.RequestInformation.HttpStatusCode != (int)HttpStatusCode.NotFound)
                    throw;
                return false;
            }
        }
        public static async Task<bool> LeaseAsync<T>(this CloudTable<T> table, T entity, TimeSpan leaseTime) where T : class, ITableEntity, ILeasable, new()
        {
            var now = DateTime.UtcNow;
            if (entity.LeaseExpire.HasValue && entity.LeaseExpire.Value >= now)
                return false;
            entity.LeaseExpire = now.Add(leaseTime);

            try
            {
                await table.UpdateAsync(entity);
                return true;
            }
            catch (StorageException e)
            {
                if (e.RequestInformation.HttpStatusCode != (int)HttpStatusCode.PreconditionFailed &&
                    e.RequestInformation.HttpStatusCode != (int)HttpStatusCode.Conflict &&
                    e.RequestInformation.HttpStatusCode != (int)HttpStatusCode.NotFound)
                    throw;
                return false;
            }
        }

        public static IEnumerable<T> BulkLease<T>(this CloudTable<T> table, IEnumerable<T> entities, TimeSpan leaseTime)
            where T : class, ITableEntity, ILeasable, new()
        {
            return table.BulkLease(entities, (item, now) => now.Add(leaseTime));
        }
        public static IAsyncEnumerable<T> BulkLeaseAsync<T>(this CloudTable<T> table, IEnumerable<T> entities, TimeSpan leaseTime)
            where T : class, ITableEntity, ILeasable, new()
        {
            return table.BulkLeaseAsync(entities, (item, now) => now.Add(leaseTime));
        }

        public static IEnumerable<T> BulkLease<T>(this CloudTable<T> table, IEnumerable<T> entities, Func<T, DateTime, DateTime> leaseFunc)
            where T : class, ITableEntity, ILeasable, new()
        {
            var now = DateTime.UtcNow;
            var list = entities.Where(item => item.LeaseExpire.GetValueOrDefault() <= now).ToList();

            foreach (var item in list)
            {
                item.LeaseExpire = leaseFunc(item, now);
            }
            return list.Chunk(100).Aggregate(Enumerable.Empty<T>(), (a, b) => a.Concat(table.bulkLease(b)));
        }
        public static async IAsyncEnumerable<T> BulkLeaseAsync<T>(this CloudTable<T> table, IEnumerable<T> entities, Func<T, DateTime, DateTime> leaseFunc)
            where T : class, ITableEntity, ILeasable, new()
        {
            var now = DateTime.UtcNow;
            var list = entities.Where(item => item.LeaseExpire.GetValueOrDefault() <= now).ToList();

            foreach (var item in list)
            {
                item.LeaseExpire = leaseFunc(item, now);
            }

            foreach (var leasables in list.Chunk(100))
            {
                var leased = await table.bulkLeaseAsync(leasables);
                foreach (var item in leased)
                {
                    yield return item;
                }
            }
        }

        static IEnumerable<T> bulkLease<T>(this CloudTable<T> table, IEnumerable<T> entities)
            where T : class, ITableEntity, ILeasable, new()
        {
            var list = entities as IList<T> ?? entities.ToList();
            try
            {
                table.BulkUpdate(list);
            }
            catch (StorageException e)
            {
                if (e.RequestInformation.HttpStatusCode != (int)HttpStatusCode.PreconditionFailed &&
                    e.RequestInformation.HttpStatusCode != (int)HttpStatusCode.Conflict)
                    throw;
                return Enumerable.Empty<T>();
            }
            return list;
        }
        static async Task<IEnumerable<T>> bulkLeaseAsync<T>(this CloudTable<T> table, IEnumerable<T> entities)
            where T : class, ITableEntity, ILeasable, new()
        {
            var list = entities as IList<T> ?? entities.ToList();
            try
            {
                await table.BulkUpdateAsync(list);
            }
            catch (StorageException e)
            {
                if (e.RequestInformation.HttpStatusCode != (int)HttpStatusCode.PreconditionFailed &&
                    e.RequestInformation.HttpStatusCode != (int)HttpStatusCode.Conflict)
                    throw;
                return Enumerable.Empty<T>();
            }
            return list;
        }

    }
}
