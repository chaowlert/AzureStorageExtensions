using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using AzureStorageExtensions;
using Microsoft.Azure.Cosmos.Table;
//namespace AzureStorageExtensions
//{
public static class CloudTableExtensions
{
    public static void SafeDelete<T>(this CloudTable<T> table, string partitionKey, string rowKey) where T : class, ITableEntity, new()
    {
        try
        {
            table.Delete(partitionKey, rowKey);
        }
        catch (StorageException ex)
        {
            if (ex.RequestInformation.HttpStatusCode != 404)
                throw;
        }
    }

    public static void SafeDelete<T>(this CloudTable<T> table, T entity) where T : class, ITableEntity, new()
    {
        try
        {
            table.Delete(entity);
        }
        catch (StorageException ex)
        {
            if (ex.RequestInformation.HttpStatusCode != 404)
                throw;
        }
    }

    public static void BulkSafeDelete<T>(this CloudTable<T> table, IEnumerable<T> entities)
        where T : class, ITableEntity, new()
    {
        foreach (var chunk in entities.Chunk(100))
        {
            table.bulkSafeDelete(chunk);
        }
    }

    static void bulkSafeDelete<T>(this CloudTable<T> table, IEnumerable<T> entities) where T : class, ITableEntity, new()
    {
        try
        {
            table.BulkDelete(entities);
        }
        catch (StorageException ex)
        {
            if (ex.RequestInformation.HttpStatusCode != 404)
                throw;
            foreach (var entity in entities)
            {
                table.SafeDelete(entity);
            }
        }
    }

    public static U AggregateSegment<T, U>(this TableQuery<T> query, U seed, Func<IEnumerable<T>, U> selector, Func<U, U, U> aggregator)
    {
        TableContinuationToken token = null;
        do
        {
            var segment = query.ExecuteSegmented(token);
            if (segment.Results.Count > 0)
            {
                var newList = selector(segment.Results);
                seed = aggregator(seed, newList);
            }
            token = segment.ContinuationToken;
        }
        while (token != null);

        return seed;
    }

    public static void ForEachSegment<T>(this TableQuery<T> query, Action<IList<T>> action)
    {
        TableContinuationToken token = null;
        do
        {
            var segment = query.ExecuteSegmented(token);
            if (segment.Results.Count > 0)
                action(segment.Results);
            token = segment.ContinuationToken;
        }
        while (token != null);
    }

    public static IEnumerable<U> MapSegment<T, U>(this TableQuery<T> query, Func<IList<T>, U> func)
    {
        TableContinuationToken token = null;
        do
        {
            var segment = query.ExecuteSegmented(token);
            if (segment.Results.Count > 0)
                yield return func(segment.Results);
            token = segment.ContinuationToken;
        }
        while (token != null);
    }

    public static bool Lease<T>(this CloudTable<T> table, string partitionKey, string rowKey, TimeSpan leaseTime) where T : class, ITableEntity, ILeasable, new()
    {
        var entity = table[partitionKey, rowKey];
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

    public static bool Lease<T>(this CloudTable<T> table, T entity, TimeSpan leaseTime) where T : class, ITableEntity, ILeasable, new()
    {
        var now = DateTime.UtcNow;
        if (entity.LeaseExpire.HasValue && entity.LeaseExpire.Value >= now)
            return false;
        entity.LeaseExpire = now.Add(leaseTime);

        try
        {
            table.Replace(entity, true);
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

    static IEnumerable<T> bulkLease<T>(this CloudTable<T> table, IEnumerable<T> entities)
        where T : class, ITableEntity, ILeasable, new()
    {
        var list = entities as IList<T> ?? entities.ToList();
        try
        {
            table.BulkReplace(list, true);
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
//}
