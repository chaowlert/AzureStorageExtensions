using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using AzureStorageExtensions;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Table.Queryable;

//namespace AzureStorageExtensions
//{
public static class CloudTableExtensions
{
    static async Task deleteIfExistsAsync(string partitionKey, string rowKey, Func<string, string, CancellationToken, Task> delete, CancellationToken cancellationToken = default(CancellationToken))
    {
        try
        {
            await delete(partitionKey, rowKey, cancellationToken);
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
        deleteIfExistsAsync(partitionKey, rowKey, delete.ToTaskFunc()).Wait();
    }
    public static Task DeleteIfExistsAsync<T>(this CloudTable<T> table, string partitionKey, string rowKey, CancellationToken cancellationToken = default(CancellationToken)) where T : class, ITableEntity, new()
    {
        return deleteIfExistsAsync(partitionKey, rowKey, table.DeleteAsync, cancellationToken);
    }

    static async Task deleteIfExistsAsync<T>(T entity, Func<T, bool, CancellationToken, Task> delete, CancellationToken cancellationToken = default(CancellationToken)) where T : class, ITableEntity, new()
    {
        try
        {
            await delete(entity, false, cancellationToken);
        }
        catch (StorageException ex)
        {
            if (ex.RequestInformation.HttpStatusCode != 404)
                throw;
        }
    }
    public static void DeleteIfExists<T>(this CloudTable<T> table, T entity) where T : class, ITableEntity, new()
    {
        Action<T, bool> delete = table.Delete;
        deleteIfExistsAsync(entity, delete.ToTaskFunc()).Wait();
    }
    public static Task DeleteIfExistsAsync<T>(this CloudTable<T> table, T entity, CancellationToken cancellationToken = default(CancellationToken)) where T : class, ITableEntity, new()
    {
        return deleteIfExistsAsync(entity, table.DeleteAsync, cancellationToken);
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
    public static async Task BulkDeleteIfExistsAsync<T>(this CloudTable<T> table, IEnumerable<T> entities, CancellationToken cancellationToken = default(CancellationToken))
        where T : class, ITableEntity, new()
    {
        foreach (var chunk in entities.Chunk(100))
        {
            await bulkDeleteIfExistsAsync(chunk, table.BulkDeleteAsync, table.DeleteIfExistsAsync, cancellationToken);
        }
    }
    static async Task bulkDeleteIfExistsAsync<T>(IEnumerable<T> entities, Func<IEnumerable<T>, CancellationToken, Task> bulkDelete, Func<T, CancellationToken, Task> deleteIfExists, CancellationToken cancellationToken = default(CancellationToken)) 
        where T : class, ITableEntity, new()
    {
        var list = entities as IList<T> ?? entities.ToList();
        try
        {
            await bulkDelete(list, cancellationToken);
        }
        catch (StorageException ex)
        {
            if (ex.RequestInformation.HttpStatusCode != 404)
                throw;
            foreach (var entity in list)
            {
                await deleteIfExists(entity, cancellationToken);
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

    public static void ForEach<T>(this TableQuery<T> query, Action<T> action)
    {
        foreach (var segment in query.IterateSegments())
        {
            segment.Results.ForEach(action);
        }
    }
    public static async Task ForEachAsync<T>(this TableQuery<T> query, Action<T> action, CancellationToken cancellationToken = default(CancellationToken))
    {
        foreach (var task in query.IterateSegmentTasks(cancellationToken))
        {
            var segment = await task;
            segment.Results.ForEach(action);
        }
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
    public static async Task ForEachSegmentAsync<T>(this TableQuery<T> query, Action<IList<T>> action, CancellationToken cancellationToken = default(CancellationToken))
    {
        foreach (var task in query.IterateSegmentTasks(cancellationToken))
        {
            var segment = await task;
            if (segment.Results.Count > 0)
            {
                action(segment.Results);
            }
        }
    }

    public static IEnumerable<Task<TableQuerySegment<T>>> IterateSegmentTasks<T>(this TableQuery<T> query, CancellationToken cancellationToken = default(CancellationToken))
    {
        TableContinuationToken token = null;
        do
        {
            var task = query.ExecuteSegmentedAsync(token, cancellationToken);
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

    public static bool Lease<T>(this CloudTable<T> table, string partitionKey, string rowKey, TimeSpan leaseTime) where T : class, ITableEntity, ILeasable, new()
    {
        var entity = table.Retrieve(partitionKey, rowKey);
        if (entity != null)
            return table.Lease(entity, leaseTime);

        entity = new T
        {
            PartitionKey = partitionKey,
            RowKey = rowKey,
            LeaseExpire = DateTime.UtcNow.Add(leaseTime),
        };
        Action<T, bool> replace = table.Insert;
        return leaseAsync(entity, leaseTime, replace.ToTaskFunc(), false).Result;
    }
    public static async Task<bool> LeaseAsync<T>(this CloudTable<T> table, string partitionKey, string rowKey, TimeSpan leaseTime, CancellationToken cancellationToken = default(CancellationToken)) where T : class, ITableEntity, ILeasable, new()
    {
        var entity = await table.RetrieveAsync(partitionKey, rowKey, cancellationToken);
        if (entity != null)
            return await table.LeaseAsync(entity, leaseTime, cancellationToken);

        entity = new T
        {
            PartitionKey = partitionKey,
            RowKey = rowKey,
            LeaseExpire = DateTime.UtcNow.Add(leaseTime),
        };
        Action<T, bool> replace = table.Insert;
        return await leaseAsync(entity, replace.ToTaskFunc(), false, cancellationToken);
    }

    static Task<bool> leaseAsync<T>(T entity, TimeSpan leaseTime, Func<T, bool, CancellationToken, Task> replace, bool checkConcurrency, CancellationToken cancellationToken = default(CancellationToken)) where T : class, ITableEntity, ILeasable, new()
    {
        var now = DateTime.UtcNow;
        if (entity.LeaseExpire.HasValue && entity.LeaseExpire.Value >= now)
            return Task.FromResult(false);
        entity.LeaseExpire = now.Add(leaseTime);
        return leaseAsync(entity, replace, checkConcurrency, cancellationToken);
    }
    static async Task<bool> leaseAsync<T>(T entity, Func<T, bool, CancellationToken, Task> replace, bool checkConcurrency, CancellationToken cancellationToken = default(CancellationToken)) where T : class, ITableEntity, ILeasable, new()
    {
        try
        {
            await replace(entity, checkConcurrency, cancellationToken);
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
    public static bool Lease<T>(this CloudTable<T> table, T entity, TimeSpan leaseTime) where T : class, ITableEntity, ILeasable, new()
    {
        Action<T, bool> replace = table.Replace;
        return leaseAsync(entity, leaseTime, replace.ToTaskFunc(), true).Result;
    }
    public static Task<bool> LeaseAsync<T>(this CloudTable<T> table, T entity, TimeSpan leaseTime, CancellationToken cancellationToken = default(CancellationToken)) where T : class, ITableEntity, ILeasable, new()
    {
        return leaseAsync(entity, leaseTime, table.ReplaceAsync, true, cancellationToken);
    }

    public static IEnumerable<T> BulkLease<T>(this CloudTable<T> table, IEnumerable<T> entities, TimeSpan leaseTime)
        where T : class, ITableEntity, ILeasable, new()
    {
        return table.BulkLease(entities, (item, now) => now.Add(leaseTime));
    }
    public static IEnumerable<T> BulkLease<T>(this CloudTable<T> table, IEnumerable<T> entities, Func<T, DateTime, DateTime> leaseFunc)
        where T : class, ITableEntity, ILeasable, new()
    {
        Action<IEnumerable<T>, bool> bulkReplace = table.BulkReplace;

        return bulkLeaseAsync(entities, leaseFunc, bulkReplace.ToTaskFunc()).Result;
    }
    public static Task<IEnumerable<T>> BulkLeaseAsync<T>(this CloudTable<T> table, IEnumerable<T> entities, TimeSpan leaseTime, CancellationToken cancellationToken = default(CancellationToken)) where T : class, ITableEntity, ILeasable, new()
    {
        return table.BulkLeaseAsync(entities, (item, now) => now.Add(leaseTime), cancellationToken);
    }
    public static Task<IEnumerable<T>> BulkLeaseAsync<T>(this CloudTable<T> table, IEnumerable<T> entities, Func<T, DateTime, DateTime> leaseFunc, CancellationToken cancellationToken = default(CancellationToken)) where T : class, ITableEntity, ILeasable, new()
    {
        return bulkLeaseAsync(entities, leaseFunc, table.BulkReplaceAsync, cancellationToken);
    }
    static async Task<IEnumerable<T>> bulkLeaseAsync<T>(IEnumerable<T> entities, Func<T, DateTime, DateTime> leaseFunc, Func<IEnumerable<T>, bool, CancellationToken, Task> bulkReplace, CancellationToken cancellationToken = default(CancellationToken)) where T : class, ITableEntity, ILeasable, new()
    {
        var now = DateTime.UtcNow;
        var list = entities.Where(item => item.LeaseExpire.GetValueOrDefault() <= now).ToList();

        foreach (var item in list)
        {
            item.LeaseExpire = leaseFunc(item, now);
        }
        var result = Enumerable.Empty<T>();
        foreach (var group in list.Chunk(100))
        {
            var leased = await bulkLeaseAsync(group, bulkReplace, cancellationToken);
            result = result.Concat(leased);
        }
        return result;
    }
    static async Task<IEnumerable<T>> bulkLeaseAsync<T>(IEnumerable<T> entities, Func<IEnumerable<T>, bool, CancellationToken, Task> bulkReplace, CancellationToken cancellationToken = default(CancellationToken)) where T : class, ITableEntity, ILeasable, new()
    {
        var list = entities as IList<T> ?? entities.ToList();
        try
        {
            await bulkReplace(list, true, cancellationToken);
            return list;
        }
        catch (StorageException e)
        {
            if (e.RequestInformation.HttpStatusCode != (int)HttpStatusCode.PreconditionFailed &&
                e.RequestInformation.HttpStatusCode != (int)HttpStatusCode.Conflict &&
                e.RequestInformation.HttpStatusCode != (int)HttpStatusCode.NotFound)
                throw;

            return Enumerable.Empty<T>();
        }
    }

    static TableQuery<T> TakeInternal<T>(this TableQuery<T> query, int count)
    {
        // ReSharper disable once ConditionIsAlwaysTrueOrFalse
        return query.Expression == null ? query.Take(count) : Queryable.Take(query, count).AsTableQuery();
    }
    public static async Task<bool> AnyAsync<T>(this TableQuery<T> query, CancellationToken cancellationToken = default(CancellationToken))
    {
        foreach (var task in query.TakeInternal(1).IterateSegmentTasks(cancellationToken))
        {
            var segment = await task;
            if (segment.Results.Count > 0)
                return true;
        }
        return false;
    }
    public static Task<bool> AnyAsync<T>(this TableQuery<T> query, Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.Where(predicate).AsTableQuery().AnyAsync(cancellationToken);
    }
    public static async Task<bool> AllAsync<T>(this TableQuery<T> query, Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default(CancellationToken))
    {
        predicate = Expression.Lambda<Func<T, bool>>(Expression.Not(predicate.Body), predicate.Parameters);
        var result = await query.AnyAsync(predicate, cancellationToken);
        return !result;
    }
    public static async Task<int> CountAsync<T>(this TableQuery<T> query, CancellationToken cancellationToken = default(CancellationToken))
    {
        int count = 0;
        foreach (var task in query.IterateSegmentTasks(cancellationToken))
        {
            var segment = await task;
            count += segment.Results.Count;
        }
        return count;
    }
    public static Task<int> CountAsync<T>(this TableQuery<T> query, Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.Where(predicate).AsTableQuery().CountAsync(cancellationToken);
    }
    public static async Task<long> LongCountAsync<T>(this TableQuery<T> query, CancellationToken cancellationToken = default(CancellationToken))
    {
        long count = 0;
        foreach (var task in query.IterateSegmentTasks(cancellationToken))
        {
            var segment = await task;
            count += segment.Results.Count;
        }
        return count;
    }
    public static Task<long> LongCountAsync<T>(this TableQuery<T> query, Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.Where(predicate).AsTableQuery().LongCountAsync(cancellationToken);
    }
    public static async Task<T> FirstAsync<T>(this TableQuery<T> query, CancellationToken cancellationToken = default(CancellationToken))
    {
        foreach (var task in query.TakeInternal(1).IterateSegmentTasks(cancellationToken))
        {
            var segment = await task;
            return segment.Results.First();
        }
        throw new InvalidOperationException("Sequence contains no elements");
    }
    public static Task<T> FirstAsync<T>(this TableQuery<T> query, Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.Where(predicate).AsTableQuery().FirstAsync(cancellationToken);
    }
    public static async Task<T> FirstOrDefaultAsync<T>(this TableQuery<T> query, CancellationToken cancellationToken = default(CancellationToken))
    {
        foreach (var task in query.TakeInternal(1).IterateSegmentTasks(cancellationToken))
        {
            var segment = await task;
            return segment.Results.FirstOrDefault();
        }
        return default(T);
    }
    public static Task<T> FirstOrDefaultAsync<T>(this TableQuery<T> query, Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.Where(predicate).AsTableQuery().FirstOrDefaultAsync(cancellationToken);
    }
    //LastAsync()
    //LastOrDefaultAsync()
    public static async Task<T> SingleAsync<T>(this TableQuery<T> query, CancellationToken cancellationToken = default(CancellationToken))
    {
        var list = await query.TakeInternal(2).ToListAsync(cancellationToken);
        return list.Single();
    }
    public static Task<T> SingleAsync<T>(this TableQuery<T> query, Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.Where(predicate).AsTableQuery().SingleAsync(cancellationToken);
    }
    public static async Task<T> SingleOrDefaultAsync<T>(this TableQuery<T> query, CancellationToken cancellationToken = default(CancellationToken))
    {
        var list = await query.TakeInternal(2).ToListAsync(cancellationToken);
        return list.SingleOrDefault();
    }
    public static Task<T> SingleOrDefaultAsync<T>(this TableQuery<T> query, Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.Where(predicate).AsTableQuery().SingleOrDefaultAsync(cancellationToken);
    }
    public static async Task<T> MinAsync<T>(this TableQuery<T> query, CancellationToken cancellationToken = default(CancellationToken))
    {
        var comparer = Comparer<T>.Default;
        var hasValue = false;
        var min = default(T);
        foreach (var task in query.IterateSegmentTasks(cancellationToken))
        {
            var segment = await task;
            var value = segment.Results.Min();
            if (hasValue)
            {
                if (comparer.Compare(value, min) < 0)
                    min = value;
            }
            else
            {
                min = value;
                hasValue = true;
            }
        }
        if (min == null)
            return default(T);
        else
            throw new InvalidOperationException("Sequence contains no elements");
    }
    public static Task<TElement> MinAsync<T, TElement>(this TableQuery<T> query, Expression<Func<T, TElement>> selector, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.Select(selector).AsTableQuery().MinAsync(cancellationToken);
    }
    public static async Task<T> MaxAsync<T>(this TableQuery<T> query, CancellationToken cancellationToken = default(CancellationToken))
    {
        var comparer = Comparer<T>.Default;
        var hasValue = false;
        var max = default(T);
        foreach (var task in query.IterateSegmentTasks(cancellationToken))
        {
            var segment = await task;
            var value = segment.Results.Min();
            if (hasValue)
            {
                if (comparer.Compare(value, max) > 0)
                    max = value;
            }
            else
            {
                max = value;
                hasValue = true;
            }
        }
        if (max == null)
            return default(T);
        else
            throw new InvalidOperationException("Sequence contains no elements");
    }
    public static Task<TElement> MaxAsync<T, TElement>(this TableQuery<T> query, Expression<Func<T, TElement>> selector, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.Select(selector).AsTableQuery().MaxAsync(cancellationToken);
    }

    public static Task<int> SumAsync(this TableQuery<int> query, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.AggregateSegmentAsync(0, list => list.Sum(), (a, b) => { checked { return a + b; } });
    }

    public static Task<int> SumAsync<T>(this TableQuery<T> query, Expression<Func<T, int>> selector, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.Select(selector).AsTableQuery().SumAsync(cancellationToken);
    }
    public static Task<int?> SumAsync(this TableQuery<int?> query, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.AggregateSegmentAsync((int?)0, list => list.Sum(), (a, b) => { checked { return a + b; } });
    }
    public static Task<int?> SumAsync<T>(this TableQuery<T> query, Expression<Func<T, int?>> selector, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.Select(selector).AsTableQuery().SumAsync(cancellationToken);
    }
    public static Task<long> SumAsync(this TableQuery<long> query, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.AggregateSegmentAsync(0L, list => list.Sum(), (a, b) => { checked { return a + b; } });
    }
    public static Task<long> SumAsync<T>(this TableQuery<T> query, Expression<Func<T, long>> selector, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.Select(selector).AsTableQuery().SumAsync(cancellationToken);
    }
    public static Task<long?> SumAsync(this TableQuery<long?> query, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.AggregateSegmentAsync((long?)0, list => list.Sum(), (a, b) => { checked { return a + b; } });
    }
    public static Task<long?> SumAsync<T>(this TableQuery<T> query, Expression<Func<T, long?>> selector, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.Select(selector).AsTableQuery().SumAsync(cancellationToken);
    }
    public static Task<float> SumAsync(this TableQuery<float> query, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.AggregateSegmentAsync(0F, list => list.Sum(), (a, b) => a + b);
    }
    public static Task<float> SumAsync<T>(this TableQuery<T> query, Expression<Func<T, float>> selector, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.Select(selector).AsTableQuery().SumAsync(cancellationToken);
    }
    public static Task<float?> SumAsync(this TableQuery<float?> query, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.AggregateSegmentAsync((float?)0, list => list.Sum(), (a, b) => a + b);
    }
    public static Task<float?> SumAsync<T>(this TableQuery<T> query, Expression<Func<T, float?>> selector, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.Select(selector).AsTableQuery().SumAsync(cancellationToken);
    }
    public static Task<double> SumAsync(this TableQuery<double> query, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.AggregateSegmentAsync(0D, list => list.Sum(), (a, b) => a + b);
    }
    public static Task<double> SumAsync<T>(this TableQuery<T> query, Expression<Func<T, double>> selector, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.Select(selector).AsTableQuery().SumAsync(cancellationToken);
    }
    public static Task<double?> SumAsync(this TableQuery<double?> query, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.AggregateSegmentAsync((double?)0, list => list.Sum(), (a, b) => a + b);
    }
    public static Task<double?> SumAsync<T>(this TableQuery<T> query, Expression<Func<T, double?>> selector, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.Select(selector).AsTableQuery().SumAsync(cancellationToken);
    }
    public static Task<decimal> SumAsync(this TableQuery<decimal> query, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.AggregateSegmentAsync(0M, list => list.Sum(), (a, b) => a + b);
    }
    public static Task<decimal> SumAsync<T>(this TableQuery<T> query, Expression<Func<T, decimal>> selector, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.Select(selector).AsTableQuery().SumAsync(cancellationToken);
    }
    public static Task<decimal?> SumAsync(this TableQuery<decimal?> query, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.AggregateSegmentAsync((decimal?)0, list => list.Sum(), (a, b) => a + b);
    }
    public static Task<decimal?> SumAsync<T>(this TableQuery<T> query, Expression<Func<T, decimal?>> selector, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.Select(selector).AsTableQuery().SumAsync(cancellationToken);
    }
    public static async Task<int> AverageAsync(this TableQuery<int> query, CancellationToken cancellationToken = default(CancellationToken))
    {
        int sum = 0;
        int count = 0;
        await query.ForEachAsync(item =>
        {
            sum += item;
            count++;
        }, cancellationToken);
        return sum / count;
    }
    public static Task<int> AverageAsync<T>(this TableQuery<T> query, Expression<Func<T, int>> selector, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.Select(selector).AsTableQuery().AverageAsync(cancellationToken);
    }
    public static async Task<int?> AverageAsync(this TableQuery<int?> query, CancellationToken cancellationToken = default(CancellationToken))
    {
        int sum = 0;
        int count = 0;
        await query.ForEachAsync(item =>
        {
            if (item.HasValue)
            {
                sum += item.Value;
                count++;
            }
        }, cancellationToken);
        if (count > 0)
            return sum / count;
        else
            throw new InvalidOperationException("Sequence contains no elements");
    }
    public static Task<int?> AverageAsync<T>(this TableQuery<T> query, Expression<Func<T, int?>> selector, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.Select(selector).AsTableQuery().AverageAsync(cancellationToken);
    }
    public static async Task<long> AverageAsync(this TableQuery<long> query, CancellationToken cancellationToken = default(CancellationToken))
    {
        long sum = 0;
        long count = 0;
        await query.ForEachAsync(item =>
        {
            sum += item;
            count++;
        }, cancellationToken);
        return sum / count;
    }
    public static Task<long> AverageAsync<T>(this TableQuery<T> query, Expression<Func<T, long>> selector, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.Select(selector).AsTableQuery().AverageAsync(cancellationToken);
    }
    public static async Task<long?> AverageAsync(this TableQuery<long?> query, CancellationToken cancellationToken = default(CancellationToken))
    {
        long sum = 0;
        long count = 0;
        await query.ForEachAsync(item =>
        {
            if (item.HasValue)
            {
                sum += item.Value;
                count++;
            }
        }, cancellationToken);
        if (count > 0)
            return sum / count;
        else
            throw new InvalidOperationException("Sequence contains no elements");
    }
    public static Task<long?> AverageAsync<T>(this TableQuery<T> query, Expression<Func<T, long?>> selector, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.Select(selector).AsTableQuery().AverageAsync(cancellationToken);
    }
    public static async Task<float> AverageAsync(this TableQuery<float> query, CancellationToken cancellationToken = default(CancellationToken))
    {
        float sum = 0;
        float count = 0;
        await query.ForEachAsync(item =>
        {
            sum += item;
            count++;
        }, cancellationToken);
        return sum / count;
    }
    public static Task<float> AverageAsync<T>(this TableQuery<T> query, Expression<Func<T, float>> selector, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.Select(selector).AsTableQuery().AverageAsync(cancellationToken);
    }
    public static async Task<float?> AverageAsync(this TableQuery<float?> query, CancellationToken cancellationToken = default(CancellationToken))
    {
        float sum = 0;
        float count = 0;
        await query.ForEachAsync(item =>
        {
            if (item.HasValue)
            {
                sum += item.Value;
                count++;
            }
        }, cancellationToken);
        if (count > 0)
            return sum / count;
        else
            throw new InvalidOperationException("Sequence contains no elements");
    }
    public static Task<float?> AverageAsync<T>(this TableQuery<T> query, Expression<Func<T, float?>> selector, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.Select(selector).AsTableQuery().AverageAsync(cancellationToken);
    }
    public static async Task<double> AverageAsync(this TableQuery<double> query, CancellationToken cancellationToken = default(CancellationToken))
    {
        double sum = 0;
        double count = 0;
        await query.ForEachAsync(item =>
        {
            sum += item;
            count++;
        }, cancellationToken);
        return sum / count;
    }
    public static Task<double> AverageAsync<T>(this TableQuery<T> query, Expression<Func<T, double>> selector, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.Select(selector).AsTableQuery().AverageAsync(cancellationToken);
    }
    public static async Task<double?> AverageAsync(this TableQuery<double?> query, CancellationToken cancellationToken = default(CancellationToken))
    {
        double sum = 0;
        double count = 0;
        await query.ForEachAsync(item =>
        {
            if (item.HasValue)
            {
                sum += item.Value;
                count++;
            }
        }, cancellationToken);
        if (count > 0)
            return sum / count;
        else
            throw new InvalidOperationException("Sequence contains no elements");
    }
    public static Task<double?> AverageAsync<T>(this TableQuery<T> query, Expression<Func<T, double?>> selector, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.Select(selector).AsTableQuery().AverageAsync(cancellationToken);
    }
    public static async Task<decimal> AverageAsync(this TableQuery<decimal> query, CancellationToken cancellationToken = default(CancellationToken))
    {
        decimal sum = 0;
        decimal count = 0;
        await query.ForEachAsync(item =>
        {
            sum += item;
            count++;
        }, cancellationToken);
        return sum / count;
    }
    public static Task<decimal> AverageAsync<T>(this TableQuery<T> query, Expression<Func<T, decimal>> selector, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.Select(selector).AsTableQuery().AverageAsync(cancellationToken);
    }
    public static async Task<decimal?> AverageAsync(this TableQuery<decimal?> query, CancellationToken cancellationToken = default(CancellationToken))
    {
        decimal sum = 0;
        decimal count = 0;
        await query.ForEachAsync(item =>
        {
            if (item.HasValue)
            {
                sum += item.Value;
                count++;
            }
        }, cancellationToken);
        if (count > 0)
            return sum / count;
        else 
            throw new InvalidOperationException("Sequence contains no elements");
    }
    public static Task<decimal?> AverageAsync<T>(this TableQuery<T> query, Expression<Func<T, decimal?>> selector, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.Select(selector).AsTableQuery().AverageAsync(cancellationToken);
    }
    public static async Task<bool> ContainAsync<T>(this TableQuery<T> query, T value, IEqualityComparer<T> comparer = null, CancellationToken cancellationToken = default(CancellationToken))
    {
        comparer = comparer ?? EqualityComparer<T>.Default;
        foreach (var task in query.IterateSegmentTasks(cancellationToken))
        {
            var segment = await task;
            if (segment.Results.Contains(value, comparer))
                return true;
        }
        return false;
    }
    public static async Task<List<T>> ToListAsync<T>(this TableQuery<T> query, CancellationToken cancellationToken = default(CancellationToken))
    {
        var list = new List<T>();
        await query.ForEachSegmentAsync(list.AddRange, cancellationToken);
        return list;
    }
    public static async Task<T[]> ToArrayAsync<T>(this TableQuery<T> query, CancellationToken cancellationToken = default(CancellationToken))
    {
        var list = await query.ToListAsync(cancellationToken);
        return list.ToArray();
    }
    public static Task<Dictionary<TKey, T>> ToDictionaryAsync<TKey, T>(this TableQuery<T> query, Func<T, TKey> keySelector, IEqualityComparer<TKey> comparer = null, CancellationToken cancellationToken = default(CancellationToken))
    {
        return query.ToDictionaryAsync(keySelector, item => item, comparer, cancellationToken);
    }
    public static async Task<Dictionary<TKey, TElement>> ToDictionaryAsync<TKey, T, TElement>(this TableQuery<T> query, Func<T, TKey> keySelector, Func<T, TElement> selector, IEqualityComparer<TKey> comparer = null, CancellationToken cancellationToken = default(CancellationToken))
    {
        var dict = new Dictionary<TKey, TElement>(comparer);
        await query.ForEachAsync(item => dict.Add(keySelector(item), selector(item)), cancellationToken);
        return dict;
    }
}
//}
