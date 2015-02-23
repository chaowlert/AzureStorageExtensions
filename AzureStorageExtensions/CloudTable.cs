using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Threading;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureStorageExtensions
{
    public class CloudTable<T> where T : class, ITableEntity, new()
    {
        public CloudTable(CloudTable cloudTable)
        {
            _cloudTable = cloudTable;
        }

        readonly CloudTable _cloudTable;
        public CloudTable CloudTableContext
        {
            get { return _cloudTable; }
        }

        public T this[string partitionKey, string rowKey]
        {
            get
            {
                return single(partitionKey, rowKey);
            }
        }

        public void Insert(T entity, bool replaceIfExists = false)
        {
            TableOperation op;
            if (replaceIfExists)
                op = TableOperation.InsertOrReplace(entity);
            else
                op = TableOperation.Insert(entity);
            _cloudTable.Execute(op);
        }

        public void BulkInsert(IEnumerable<T> entities, bool replaceIfExists = false)
        {
            var op = new TableBatchOperation();

            foreach (var item in entities)
            {
                if (replaceIfExists)
                    op.InsertOrReplace(item);
                else
                    op.Insert(item);
                if (op.Count < 100)
                    continue;
                _cloudTable.ExecuteBatch(op);
                op.Clear();
            }
            if (op.Count > 0)
                _cloudTable.ExecuteBatch(op);
        }

        public TableQuery<T> Query()
        {
            return _cloudTable.CreateQuery<T>();
        }

        T single(string partitionKey, string rowKey)
        {
            var op = TableOperation.Retrieve<T>(partitionKey, rowKey);
            var result = _cloudTable.Execute(op);
            return (T)result.Result;
        }

        public T Update(string partitionKey, string rowKey, Action<T> action, bool createIfNotExists = false)
        {
            var policy = _cloudTable.ServiceClient.DefaultRequestOptions.RetryPolicy;

            var retry = 0;
            while (true)
            {
                var obj = single(partitionKey, rowKey);
                var isNew = false;
                if (obj == null)
                {
                    if (createIfNotExists)
                    {
                        obj = new T
                        {
                            PartitionKey = partitionKey,
                            RowKey = rowKey
                        };
                        isNew = true;
                    }
                    else 
                    {
                        throw new StorageException(
                            new RequestResult
                            {
                                HttpStatusCode = (int)HttpStatusCode.NotFound
                            },
                            string.Format("Error update: Not Found\nPartitionKey: {0}\nRowKey: {1}", partitionKey, rowKey),
                            null);
                    }
                }
                action(obj);

                try
                {
                    TableOperation op;
                    if (isNew)
                        op = TableOperation.Insert(obj);
                    else
                        op = TableOperation.Replace(obj);
                    _cloudTable.Execute(op);
                    return obj;
                }
                catch (StorageException e)
                {
                    if (e.RequestInformation.HttpStatusCode != (int)HttpStatusCode.PreconditionFailed &&
                        e.RequestInformation.HttpStatusCode != (int)HttpStatusCode.Conflict)
                        throw;
                    TimeSpan delay;
                    if (!policy.ShouldRetry(retry++, 0, e, out delay, null))
                        throw;
                    Thread.Sleep(delay);
                }
            }
        }

        public bool CheckUpdate(string partitionKey, string rowKey, Func<T, bool> func, bool createIfNotExists = false)
        {
            var policy = _cloudTable.ServiceClient.DefaultRequestOptions.RetryPolicy;

            var retry = 0;
            while (true)
            {
                var obj = single(partitionKey, rowKey);
                var isNew = false;
                if (obj == null)
                {
                    if (createIfNotExists)
                    {
                        obj = new T
                        {
                            PartitionKey = partitionKey,
                            RowKey = rowKey
                        };
                        isNew = true;
                    }
                    else
                    {
                        throw new StorageException(
                            new RequestResult
                            {
                                HttpStatusCode = (int)HttpStatusCode.NotFound
                            },
                            string.Format("Error update: Not Found\nPartitionKey: {0}\nRowKey: {1}", partitionKey, rowKey),
                            null);
                    }
                }
                if (!func(obj))
                    return false;

                try
                {
                    TableOperation op;
                    if (isNew)
                        op = TableOperation.Insert(obj);
                    else
                        op = TableOperation.Replace(obj);
                    _cloudTable.Execute(op);
                    return true;
                }
                catch (StorageException e)
                {
                    if (e.RequestInformation.HttpStatusCode != (int)HttpStatusCode.PreconditionFailed &&
                        e.RequestInformation.HttpStatusCode != (int)HttpStatusCode.Conflict)
                        throw;
                    TimeSpan delay;
                    if (!policy.ShouldRetry(retry++, 0, e, out delay, null))
                        throw;
                    Thread.Sleep(delay);
                }
            }
        }

        void bulkApply(Dictionary<string, T> dict, List<T> updated)
        {
            var op = new TableBatchOperation();

            foreach (var item in updated)
            {
                T value;
                if (dict.TryGetValue(item.RowKey, out value))
                {
                    dict.Remove(item.RowKey);
                    if (item.ApplyTo(value))
                        op.InsertOrReplace(item);
                }
                else
                    op.InsertOrReplace(item);
                if (op.Count < 100)
                    continue;
                _cloudTable.ExecuteBatch(op);
                op.Clear();
            }
            foreach (var item in dict)
            {
                item.Value.ETag = "*";
                op.Delete(item.Value);
                if (op.Count < 100)
                    continue;
                _cloudTable.ExecuteBatch(op);
                op.Clear();
            }
            if (op.Count > 0)
                _cloudTable.ExecuteBatch(op);
        }

        public void BulkApply(string partitionKey, List<T> updated)
        {
            var dict = (from item in Query()
                        where item.PartitionKey == partitionKey
                        select item).ToDictionary(item => item.RowKey);
            bulkApply(dict, updated);
        }

        public void BulkApply(string partitionKey, Func<Dictionary<string, T>, List<T>> func)
        {
            var dict = (from item in Query()
                        where item.PartitionKey == partitionKey
                        select item).ToDictionary(item => item.RowKey);
            var updated = func(dict);
            bulkApply(dict, updated);
        }

        public void BulkUpdate(string partitionKey, Func<string, Dictionary<string, T>, Dictionary<string, T>> func)
        {
            var policy = _cloudTable.ServiceClient.DefaultRequestOptions.RetryPolicy;

            var retry = 0;
            while (true)
            {
                var dict = (from item in Query()
                            where item.PartitionKey == partitionKey
                            select item).ToDictionary(item => item.RowKey);
                var updated = func(partitionKey, dict);

                try
                {
                    var op = new TableBatchOperation();
                    foreach (var item in updated)
                    {
                        if (dict.ContainsKey(item.Key))
                            op.Replace(item.Value);
                        else
                            op.Insert(item.Value);
                        if (op.Count < 100)
                            continue;
                        _cloudTable.ExecuteBatch(op);
                        op.Clear();
                    }
                    if (op.Count > 0)
                        _cloudTable.ExecuteBatch(op);
                    return;
                }
                catch (StorageException e)
                {
                    if (e.RequestInformation.HttpStatusCode != (int)HttpStatusCode.PreconditionFailed &&
                        e.RequestInformation.HttpStatusCode != (int)HttpStatusCode.Conflict)
                        throw;
                    TimeSpan delay;
                    if (!policy.ShouldRetry(retry++, 0, e, out delay, null))
                        throw;
                    Thread.Sleep(delay);
                }
            }
        }

        public void BulkUpdate(string partitionKey, IEnumerable<string> rowKeys, Func<string, Dictionary<string, T>, Dictionary<string, T>> func)
        {
            Expression<Func<T, bool>> lambda = null;
            var rowKeyList = rowKeys as ICollection<string> ?? rowKeys.ToList();
            if (rowKeyList.Count > 0)
            {
                var p = Expression.Parameter(typeof(T), "p");
                var predicate = rowKeyList.Select(
                    item => Expression.Equal(Expression.Property(p, "RowKey"), Expression.Constant(item)))
                                      .AggregateBalance(Expression.OrElse);
                lambda = Expression.Lambda<Func<T, bool>>(predicate, p);
            }

            BulkUpdate(partitionKey, lambda, func);
        }

        public void BulkUpdate(string partitionKey, Expression<Func<T, bool>> where, Func<string, Dictionary<string, T>, Dictionary<string, T>> func)
        {
            var policy = _cloudTable.ServiceClient.DefaultRequestOptions.RetryPolicy;
            var retry = 0;
            while (true)
            {
                Dictionary<string, T> dict;
                if (where == null)
                    dict = new Dictionary<string, T>();
                else
                {
                    var linq = (from item in Query()
                                where item.PartitionKey == partitionKey
                                select item);
                    linq = linq.Where(where);
                    dict = linq.ToDictionary(item => item.RowKey);
                }
                var updated = func(partitionKey, dict);

                try
                {
                    var op = new TableBatchOperation();
                    foreach (var item in updated)
                    {
                        if (dict.ContainsKey(item.Key))
                            op.Replace(item.Value);
                        else
                            op.Insert(item.Value);
                        if (op.Count < 100)
                            continue;
                        _cloudTable.ExecuteBatch(op);
                        op.Clear();
                    }
                    if (op.Count > 0)
                        _cloudTable.ExecuteBatch(op);
                    return;
                }
                catch (StorageException e)
                {
                    if (e.RequestInformation.HttpStatusCode != (int)HttpStatusCode.PreconditionFailed &&
                        e.RequestInformation.HttpStatusCode != (int)HttpStatusCode.Conflict)
                        throw;
                    TimeSpan delay;
                    if (!policy.ShouldRetry(retry++, 0, e, out delay, null))
                        throw;
                    Thread.Sleep(delay);
                }
            }
        }

        public void Delete(T entity)
        {
            entity.ETag = "*";
            var op = TableOperation.Delete(entity);
            _cloudTable.Execute(op);
        }

        public void Delete(string partitionKey, string rowKey)
        {
            var entity = new T
            {
                PartitionKey = partitionKey,
                RowKey = rowKey, 
                ETag = "*",
            };
            var op = TableOperation.Delete(entity);
            _cloudTable.Execute(op);
        }

        public void BulkDelete(IEnumerable<T> entities)
        {
            var op = new TableBatchOperation();

            foreach (var item in entities)
            {
                item.ETag = "*";
                op.Delete(item);
                if (op.Count < 100)
                    continue;
                _cloudTable.ExecuteBatch(op);
                op.Clear();
            }
            if (op.Count > 0)
                _cloudTable.ExecuteBatch(op);
        }

        public void BulkDelete(string partitionKey, Expression<Func<T, bool>> predicate)
        {
            var linq = (from item in Query()
                        where item.PartitionKey == partitionKey
                        select item);
            linq = linq.Where(predicate);
            var entities = linq.ToList();

            var op = new TableBatchOperation();

            foreach (var item in entities)
            {
                item.ETag = "*";
                op.Delete(item);
                if (op.Count < 100)
                    continue;
                _cloudTable.ExecuteBatch(op);
                op.Clear();
            }
            if (op.Count > 0)
                _cloudTable.ExecuteBatch(op);
        }

        public void Replace(T entity, bool checkConcurrency = false)
        {
            if (!checkConcurrency)  
                entity.ETag = "*";
            var op = TableOperation.Replace(entity);
            _cloudTable.Execute(op);
        }

        public void BulkReplace(IEnumerable<T> entities, bool checkConcurrency = false)
        {
            var op = new TableBatchOperation();

            foreach (var item in entities)
            {
                if (!checkConcurrency)
                    item.ETag = "*";
                op.Replace(item);
                if (op.Count < 100)
                    continue;
                _cloudTable.ExecuteBatch(op);
                op.Clear();
            }
            if (op.Count > 0)
                _cloudTable.ExecuteBatch(op);
        }

        public void Merge(string partitionKey, string rowKey, Action<DynamicTableEntity> action, bool createIfNotExists = false)
        {
            var entity = new DynamicTableEntity
            {
                PartitionKey = partitionKey,
                RowKey = rowKey,
                ETag = "*",
            };
            action(entity);
            TableOperation op;
            if (createIfNotExists)
                op = TableOperation.InsertOrMerge(entity);
            else
                op = TableOperation.Merge(entity);
            _cloudTable.Execute(op);
        }

        public void BulkMerge(string partitionKey, Action<DynamicTableEntity> action, bool createIfNotExists = false)
        {
            var rowKeys = (from item in Query()
                           where item.PartitionKey == partitionKey
                           select item.RowKey).ToList();

            var op = new TableBatchOperation();
            foreach (var rowKey in rowKeys)
            {
                var item = new DynamicTableEntity
                {
                    PartitionKey = partitionKey,
                    RowKey = rowKey,
                    ETag = "*",
                };
                action(item);
                op.Merge(item);
                if (op.Count < 100)
                    continue;
                _cloudTable.ExecuteBatch(op);
                op.Clear();
            }
            if (op.Count > 0)
                _cloudTable.ExecuteBatch(op);
        }

        public bool Lease<U>(U entity, TimeSpan leaseTime) where U : T, ILeasable
        {
            var now = DateTime.UtcNow;
            if (entity.LeaseExpire.HasValue && entity.LeaseExpire.Value >= now)
                return false;
            entity.LeaseExpire = DateTime.UtcNow.Add(leaseTime);

            try
            {
                Replace(entity, true);
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
    }
}