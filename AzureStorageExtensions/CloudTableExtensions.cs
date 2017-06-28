using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Threading.Tasks;
using AzureStorageExtensions;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Table.Queryable;

//namespace AzureStorageExtensions
//{
public static class CloudTableExtensions
{
    static async Task deleteIfExistsAsync<T>(string partitionKey, string rowKey, Func<string, string, Task> delete) where T : class, ITableEntity, new()
    {
        try
        {
            await delete(partitionKey, rowKey);
        }
        catch (StorageException ex)
        {
            if (ex.RequestInformation.HttpStatusCode != 404)
                throw;
        }
    }
    public static void DeleteIfExists<T>(this CloudTable<T> table, string partitionKey, string rowKey) where T : class, ITableEntity, new()
    {
        Action<string, string> delete = table.Delete;
        deleteIfExistsAsync<T>(partitionKey, rowKey, delete.ToTaskFunc()).Wait();
    }
    public static Task DeleteIfExistsAsync<T>(this CloudTable<T> table, string partitionKey, string rowKey) where T : class, ITableEntity, new()
    {
        return deleteIfExistsAsync<T>(partitionKey, rowKey, table.DeleteAsync);
    }

    public static void DeleteIfExists<T>(this CloudTable<T> table, T entity) where T : class, ITableEntity, new()
    {
        table.DeleteIfExists(entity.PartitionKey, entity.RowKey);
    }
    public static Task DeleteIfExistsAsync<T>(this CloudTable<T> table, T entity) where T : class, ITableEntity, new()
    {
        return table.DeleteIfExistsAsync(entity.PartitionKey, entity.RowKey);
    }

    public static void BulkDeleteIfExists<T>(this CloudTable<T> table, IEnumerable<T> entities)
        where T : class, ITableEntity, new()
    {
        Action<IEnumerable<T>> bulkDelete = table.BulkDelete;
        Action<T> deleteIfExists = table.DeleteIfExists;
        foreach (var chunk in entities.Chunk(100))
        {
            bulkDeleteIfExistsAsync(chunk, bulkDelete.ToTaskFunc(), deleteIfExists.ToTaskFunc()).Wait();
        }
    }
    public static async Task BulkDeleteIfExistsAsync<T>(this CloudTable<T> table, IEnumerable<T> entities)
        where T : class, ITableEntity, new()
    {
        foreach (var chunk in entities.Chunk(100))
        {
            await bulkDeleteIfExistsAsync(chunk, table.BulkDeleteAsync, table.DeleteIfExistsAsync);
        }
    }
    static async Task bulkDeleteIfExistsAsync<T>(IEnumerable<T> entities, Func<IEnumerable<T>, Task> bulkDelete, Func<T, Task> deleteIfExists) 
        where T : class, ITableEntity, new()
    {
        var list = entities as IList<T> ?? entities.ToList();
        try
        {
            await bulkDelete(list);
        }
        catch (StorageException ex)
        {
            if (ex.RequestInformation.HttpStatusCode != 404)
                throw;
            foreach (var entity in list)
            {
                await deleteIfExists(entity);
            }
        }
    }

    public static U AggregateSegment<T, U>(this TableQuery<T> query, U seed, Func<IEnumerable<T>, U> selector, Func<U, U, U> aggregator)
    {
        foreach (var segment in query.IterateSegments())
        {
            if (segment.Results.Count > 0)
            {
                var newList = selector(segment.Results);
                seed = aggregator(seed, newList);
            }
        }
        return seed;
    }
    public static async Task<U> AggregateSegmentAsync<T, U>(this TableQuery<T> query, U seed, Func<IEnumerable<T>, U> selector, Func<U, U, U> aggregator)
    {
        foreach (var task in query.IterateSegmentTasks())
        {
            var segment = await task;
            if (segment.Results.Count > 0)
            {
                var newList = selector(segment.Results);
                seed = aggregator(seed, newList);
            }
        }
        return seed;
    }

    public static void ForEachSegment<T>(this TableQuery<T> query, Action<IList<T>> action)
    {
        foreach (var segment in query.IterateSegments())
        {
            if (segment.Results.Count > 0)
            {
                action(segment.Results);
            }
        }
    }
    public static async Task ForEachSegmentAsync<T>(this TableQuery<T> query, Action<IList<T>> action)
    {
        foreach (var task in query.IterateSegmentTasks())
        {
            var segment = await task;
            if (segment.Results.Count > 0)
            {
                action(segment.Results);
            }
        }
    }

    public static IEnumerable<Task<TableQuerySegment<T>>> IterateSegmentTasks<T>(this TableQuery<T> query)
    {
        TableContinuationToken token = null;
        do
        {
            var task = query.ExecuteSegmentedAsync(token);
            yield return task;
            var segment = task.Result;
            token = segment.ContinuationToken;
        }
        while (token != null);
    }
    public static IEnumerable<TableQuerySegment<T>> IterateSegments<T>(this TableQuery<T> query)
    {
        TableContinuationToken token = null;
        do
        {
            var segment = query.ExecuteSegmented(token);
            yield return segment;
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

    public static async Task<bool> AnyAsync<T>(this TableQuery<T> query)
    {
        foreach (var task in query.Take(1).IterateSegmentTasks())
        {
            var segment = await task;
            if (segment.Results.Count > 0)
                return true;
        }
        return false;
    }
    public static async Task<bool> AnyAsync<T>(this TableQuery<T> query, Func<T, bool> predicate)
    {
        foreach (var task in query.IterateSegmentTasks())
        {
            var segment = await task;
            if (segment.Results.Any(predicate))
                return true;
        }
        return false;
    }

    public static async Task<bool> AllAsync<T>(this TableQuery<T> query, Func<T, bool> predicate)
    {
        foreach (var task in query.IterateSegmentTasks())
        {
            var segment = await task;
            if (!segment.Results.All(predicate))
                return false;
        }
        return true;
    }
    //CountAsync()
    //LongCountAsync()
    //FirstAsync()
    //FirstOrDefaultAsync()
    //LastAsync()
    //LastOrDefaultAsync()
    //SingleAsync()
    //SingleOrDefaultAsync()
    //MinAsync()
    //MaxAsync()
    //SumAsync()
    //AverageAsync()
    //ContainsAsync()
    //ToListAsync()
    //ToArrayAsync()
    //LoadAsync()
    //ToDictionaryAsync()
    //ForEachAsync()
}
//}
