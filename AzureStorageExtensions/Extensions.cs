using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Reflection;
using AzureStorageExtensions;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Blob.Protocol;
using Microsoft.WindowsAzure.Storage.Table;

//namespace AzureStorageExtensions
//{
public static class PublicExtensions
{
    public static void SafeDelete(this CloudBlockBlob blob)
    {
        try
        {
            blob.Delete();
        }
        catch (StorageException ex)
        {
            if (ex.RequestInformation.HttpStatusCode != 404)
                throw;
            if (ex.RequestInformation.ExtendedErrorInformation == null || ex.RequestInformation.ExtendedErrorInformation.ErrorCode == BlobErrorCodeStrings.BlobNotFound)
                return;
            throw;
        }
    }

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
static class Extensions
{
    public static IEnumerable<IEnumerable<T>> Chunk<T>(this IEnumerable<T> source, int chunksize)
    {
        var list = source as IList<T> ?? source.ToList();
        var pos = 0;
        while (list.Count > pos)
        {
            yield return list.Skip(pos).Take(chunksize);
            pos += chunksize;
        }
    }

    public static T AggregateBalance<T>(this IEnumerable<T> source, Func<T, T, T> func)
    {
        while (true)
        {
            var list = new List<T>();
            var previous = default(T);
            var pair = false;
            foreach (var item in source)
            {
                if (pair)
                    list.Add(func(previous, item));
                else
                    previous = item;
                pair = !pair;
            }
            if (pair)
                list.Add(previous);

            if (list.Count == 0)
                throw new InvalidOperationException("No element");
            if (list.Count == 1)
                return list[0];
            source = list;
        }
    }

    public static U GetValueOrDefault<T, U>(this IDictionary<T, U> dict, T key)
    {
        U value;
        return dict.TryGetValue(key, out value) ? value : default(U);
    }

    public static T[] Concat<T>(params T[][] list)
    {
        var result = new T[list.Sum(a => a.Length)];
        int offset = 0;
        foreach (T[] array in list)
        {
            array.CopyTo(result, offset);
            offset += array.Length;
        }
        return result;
    }

    public static bool ApplyTo<T>(this T src, T dest)
    {
        return Extensions<T>.Apply(src, dest);
    }
}

static class Extensions<T>
{
    public static readonly Func<T, T, bool> Apply = CreateApplyFunc();

    private static Func<T, T, bool> CreateApplyFunc()
    {
        var type = typeof(T);
        var src = Expression.Parameter(type, "src");
        var dest = Expression.Parameter(type, "dest");

        var list = new List<Expression>();
        var isUpdated = Expression.Variable(typeof(bool), "isUpdated");
        list.Add(Expression.Assign(isUpdated, Expression.Constant(false)));

        foreach (var prop in type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.GetProperty | BindingFlags.SetProperty))
        {
            switch (prop.Name)
            {
                case "PartitionKey":
                case "RowKey":
                case "Timestamp":
                case "ETag":
                    continue;
            }

            var left = Expression.Property(src, prop);
            var right = Expression.Property(dest, prop);
            var test = Expression.NotEqual(right, left);
            var assign = Expression.Assign(right, left);
            var updated = Expression.Assign(isUpdated, Expression.Constant(true));

            list.Add(Expression.IfThen(test, Expression.Block(assign, updated)));
        }

        list.Add(isUpdated);
        var body = Expression.Block(new[] { isUpdated }, list);
        return Expression.Lambda<Func<T, T, bool>>(body, src, dest).Compile();
    }
}
//}
