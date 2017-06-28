using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureStorageExtensions
{
    public class CloudTable<T> where T : class, ITableEntity, new()
    {
        public CloudTable(CloudTable cloudTable)
        {
            CloudTableContext = cloudTable;
        }

        public CloudTable CloudTableContext { get; }

        public T this[string partitionKey, string rowKey] => Retrieve(partitionKey, rowKey);

        private static TableOperation getInsertOperation(T entity, bool replaceIfExists = false)
        {
            return replaceIfExists ? TableOperation.InsertOrReplace(entity) : TableOperation.Insert(entity);
        }
        public void Insert(T entity, bool replaceIfExists = false)
        {
            var op = getInsertOperation(entity, replaceIfExists);
            CloudTableContext.Execute(op);
        }
        public Task InsertAsync(T entity, bool replaceIfExists = false)
        {
            var op = getInsertOperation(entity, replaceIfExists);
            return CloudTableContext.ExecuteAsync(op);
        }

        static IEnumerable<TableBatchOperation> getBulkOperations<U>(TableBatchOperation op, Action<U> action, IEnumerable<U> entities, bool checkConcurrency = false) where U : class, ITableEntity
        {
            foreach (var item in entities)
            {
                if (!checkConcurrency)
                    item.ETag = "*";
                action(item);
                if (op.Count < 100)
                    continue;
                yield return op;
                op.Clear();
            }
            if (op.Count > 0)
                yield return op;
        }
        internal void Bulk<U>(Func<TableBatchOperation, Action<U>> func, IEnumerable<U> entities, bool checkConcurrency = false) where U : class, ITableEntity
        {
            var op = new TableBatchOperation();
            var action = func(op);
            foreach (var batchOp in getBulkOperations(op, action, entities, checkConcurrency))
                CloudTableContext.ExecuteBatch(batchOp);
        }
        internal async Task BulkAsync<U>(Func<TableBatchOperation, Action<U>> func, IEnumerable<U> entities, bool checkConcurrency = false) where U : class, ITableEntity
        {
            var op = new TableBatchOperation();
            var action = func(op);
            foreach (var batchOp in getBulkOperations(op, action, entities, checkConcurrency))
                await CloudTableContext.ExecuteBatchAsync(batchOp);
        }

        public void BulkInsert(IEnumerable<T> entities, bool replaceIfExists = false)
        {
            Bulk(op => replaceIfExists ? new Action<T>(op.InsertOrReplace) : op.Insert, entities);
        }
        public Task BulkInsertAsync(IEnumerable<T> entities, bool replaceIfExists = false)
        {
            return BulkAsync(op => replaceIfExists ? new Action<T>(op.InsertOrReplace) : op.Insert, entities);
        }

        public TableQuery<T> Query()
        {
            return CloudTableContext.CreateQuery<T>();
        }

        public T Retrieve(string partitionKey, string rowKey)
        {
            var op = TableOperation.Retrieve<T>(partitionKey, rowKey);
            var result = CloudTableContext.Execute(op);
            return (T)result.Result;
        }
        public async Task<T> RetrieveAsync(string partitionKey, string rowKey)
        {
            var op = TableOperation.Retrieve<T>(partitionKey, rowKey);
            var result = await CloudTableContext.ExecuteAsync(op);
            return (T)result.Result;
        }

        async Task<U> retryAsync<U>(Func<Task<U>> func, bool sync = false)
        {
            var policy = CloudTableContext.ServiceClient.DefaultRequestOptions.RetryPolicy;
            var retry = 0;
            while (true)
            {
                try
                {
                    return await func();
                }
                catch (StorageException e)
                {
                    if (e.RequestInformation.HttpStatusCode != (int)HttpStatusCode.PreconditionFailed &&
                        e.RequestInformation.HttpStatusCode != (int)HttpStatusCode.Conflict)
                        throw;
                    if (!policy.ShouldRetry(retry++, 0, e, out TimeSpan delay, null))
                        throw;
                    if (sync)
                        Thread.Sleep(delay);
                    else
                        await Task.Delay(delay);
                }
            }
        }

        internal Task<TableResult> executeSync(TableOperation op)
        {
            var result = CloudTableContext.Execute(op);
            return Task.FromResult(result);
        }
        public T Update(string partitionKey, string rowKey, Action<T> action, bool createIfNotExists = false)
        {
            return retryAsync(getUpdateAction(partitionKey, rowKey,
                obj =>
                {
                    action(obj);
                    return true;
                },
                createIfNotExists,
                executeSync,
                (obj, updated) => obj), sync: true).Result;
        }
        public Task<T> UpdateAsync(string partitionKey, string rowKey, Action<T> action, bool createIfNotExists = false)
        {
            return retryAsync(getUpdateAction(partitionKey, rowKey,
                obj =>
                {
                    action(obj);
                    return true;
                },
                createIfNotExists,
                CloudTableContext.ExecuteAsync,
                (obj, updated) => obj));
        }

        static Func<Task<U>> getUpdateAction<U>(string partitionKey, string rowKey,Func<T, bool> func, bool createIfNotExists, Func<TableOperation, Task<TableResult>> execute, Func<T, bool, U> selector)
        {
            return async () =>
            {
                var retrieveOp = TableOperation.Retrieve<T>(partitionKey, rowKey);
                var result = await execute(retrieveOp);
                var obj = (T)result.Result;
                bool isNew = false;
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
                            $"Error update: Not Found\nPartitionKey: {partitionKey}\nRowKey: {rowKey}",
                            null);
                    }
                }
                if (!func(obj))
                    return selector(obj, false);

                var updateOp = isNew ? TableOperation.Insert(obj) : TableOperation.Replace(obj);
                await execute(updateOp);
                return selector(obj, true);
            };
        }
        public bool CheckUpdate(string partitionKey, string rowKey, Func<T, bool> func, bool createIfNotExists = false)
        {
            return retryAsync(getUpdateAction(partitionKey, rowKey, func, createIfNotExists, executeSync, (obj, updated) => updated), sync: true).Result;
        }
        public Task<bool> CheckUpdateAsync(string partitionKey, string rowKey, Func<T, bool> func, bool createIfNotExists = false)
        {
            return retryAsync(getUpdateAction(partitionKey, rowKey, func, createIfNotExists, CloudTableContext.ExecuteAsync, (obj, updated) => updated));
        }

        static Func<TableBatchOperation, Action<T>> getApplyOperations(Dictionary<string, T> dict)
        {
            return op => item =>
            {
                if (dict.TryGetValue(item.RowKey, out T value))
                {
                    dict.Remove(item.RowKey);
                    if (value.ApplyTo(item))
                        op.InsertOrReplace(item);
                }
                else
                    op.InsertOrReplace(item);
            };
        }
        void bulkApply(Dictionary<string, T> dict, IEnumerable<T> updated)
        {
            Bulk(getApplyOperations(dict), updated, true);
            Bulk(op => op.Delete, dict.Values);
        }
        async Task bulkApplyAsync(Dictionary<string, T> dict, IEnumerable<T> updated)
        {
            await BulkAsync(getApplyOperations(dict), updated, true);
            await BulkAsync(op => op.Delete, dict.Values);
        }

        public void BulkApply(string partitionKey, List<T> updated)
        {
            var dict = (from item in Query()
                        where item.PartitionKey == partitionKey
                        select item).ToDictionary(item => item.RowKey);
            bulkApply(dict, updated);
        }
        public Task BulkApplyAsync(string partitionKey, List<T> updated)
        {
            var dict = (from item in Query()
                        where item.PartitionKey == partitionKey
                        select item).ToDictionary(item => item.RowKey);
            return bulkApplyAsync(dict, updated);
        }

        public void BulkApply(string partitionKey, Func<Dictionary<string, T>, List<T>> func)
        {
            var dict = (from item in Query()
                        where item.PartitionKey == partitionKey
                        select item).ToDictionary(item => item.RowKey);
            var updated = func(dict);
            bulkApply(dict, updated);
        }
        public Task BulkApplyAsync(string partitionKey, Func<Dictionary<string, T>, List<T>> func)
        {
            var dict = (from item in Query()
                        where item.PartitionKey == partitionKey
                        select item).ToDictionary(item => item.RowKey);
            var updated = func(dict);
            return bulkApplyAsync(dict, updated);
        }

        static Func<TableBatchOperation, Action<T>> getUpdateOperations(Dictionary<string, T> dict)
        {
            return op => item =>
            {
                if (dict.ContainsKey(item.RowKey))
                    op.Replace(item);
                else
                    op.Insert(item);
            };
        }
        public void BulkUpdate(string partitionKey, Func<string, Dictionary<string, T>, Dictionary<string, T>> func)
        {
            retryAsync(() =>
            {
                var dict = (from item in Query()
                            where item.PartitionKey == partitionKey
                            select item).ToDictionary(item => item.RowKey);
                var updated = func(partitionKey, dict);

                Bulk(getUpdateOperations(dict), updated.Values, true);
                return Task.FromResult(0);
            }, sync: true).Wait();
        }
        public Task BulkUpdateAsync(string partitionKey, Func<string, Dictionary<string, T>, Dictionary<string, T>> func)
        {
            return retryAsync(async () =>
            {
                var dict = (from item in Query()
                            where item.PartitionKey == partitionKey
                            select item).ToDictionary(item => item.RowKey);
                var updated = func(partitionKey, dict);

                await BulkAsync(getUpdateOperations(dict), updated.Values, true);
                return 0;
            });
        }

        static Expression<Func<T, bool>> createLambda(IEnumerable<string> rowKeys)
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
            return lambda;
        }
        public void BulkUpdate(string partitionKey, IEnumerable<string> rowKeys, Func<string, Dictionary<string, T>, Dictionary<string, T>> func)
        {
            var lambda = createLambda(rowKeys);
            BulkUpdate(partitionKey, lambda, func);
        }
        public Task BulkUpdateAsync(string partitionKey, IEnumerable<string> rowKeys, Func<string, Dictionary<string, T>, Dictionary<string, T>> func)
        {
            var lambda = createLambda(rowKeys);
            return BulkUpdateAsync(partitionKey, lambda, func);
        }

        public void BulkUpdate(string partitionKey, Expression<Func<T, bool>> where, Func<string, Dictionary<string, T>, Dictionary<string, T>> func)
        {
            retryAsync(() =>
            {
                Dictionary<string, T> dict;
                if (where == null)
                    dict = new Dictionary<string, T>();
                else
                {
                    var linq = from item in Query()
                               where item.PartitionKey == partitionKey
                               select item;
                    linq = linq.Where(where);
                    dict = linq.ToDictionary(item => item.RowKey);
                }
                var updated = func(partitionKey, dict);

                Bulk(getUpdateOperations(dict), updated.Values, true);
                return Task.FromResult(0);
            }, sync: true).Wait();
        }
        public Task BulkUpdateAsync(string partitionKey, Expression<Func<T, bool>> where, Func<string, Dictionary<string, T>, Dictionary<string, T>> func)
        {
            return retryAsync(async () =>
            {
                Dictionary<string, T> dict;
                if (where == null)
                    dict = new Dictionary<string, T>();
                else
                {
                    var linq = from item in Query()
                               where item.PartitionKey == partitionKey
                               select item;
                    linq = linq.Where(where);
                    dict = linq.ToDictionary(item => item.RowKey);
                }
                var updated = func(partitionKey, dict);

                await BulkAsync(getUpdateOperations(dict), updated.Values, true);
                return 0;
            });
        }

        public void Delete(T entity)
        {
            var op = getDeleteOperation(entity.PartitionKey, entity.RowKey);
            CloudTableContext.Execute(op);
        }
        public Task DeleteAsync(T entity)
        {
            var op = getDeleteOperation(entity.PartitionKey, entity.RowKey);
            return CloudTableContext.ExecuteAsync(op);
        }

        internal static TableOperation getDeleteOperation(string partitionKey, string rowKey)
        {
            var entity = new T
            {
                PartitionKey = partitionKey,
                RowKey = rowKey,
                ETag = "*",
            };
            return TableOperation.Delete(entity);
        }
        public void Delete(string partitionKey, string rowKey)
        {
            var op = getDeleteOperation(partitionKey, rowKey);
            CloudTableContext.Execute(op);
        }
        public Task DeleteAsync(string partitionKey, string rowKey)
        {
            var op = getDeleteOperation(partitionKey, rowKey);
            return CloudTableContext.ExecuteAsync(op);
        }

        public void BulkDelete(IEnumerable<T> entities)
        {
            Bulk(op => op.Delete, entities);
        }
        public Task BulkDeleteAsync(IEnumerable<T> entities)
        {
            return BulkAsync(op => op.Delete, entities);
        }

        public void BulkDelete(string partitionKey, Expression<Func<T, bool>> predicate)
        {
            var linq = from item in Query()
                       where item.PartitionKey == partitionKey
                       select item;
            linq = linq.Where(predicate);
            var entities = linq.ToList();

            BulkDelete(entities);
        }
        public Task BulkDeleteAsync(string partitionKey, Expression<Func<T, bool>> predicate)
        {
            var linq = from item in Query()
                       where item.PartitionKey == partitionKey
                       select item;
            linq = linq.Where(predicate);
            var entities = linq.ToList();

            return BulkDeleteAsync(entities);
        }

        static TableOperation getReplaceOperation(T entity, bool checkConcurrency = false)
        {
            if (!checkConcurrency)
                entity.ETag = "*";
            return TableOperation.Replace(entity);
        }
        public void Replace(T entity, bool checkConcurrency = false)
        {
            var op = getReplaceOperation(entity, checkConcurrency);
            CloudTableContext.Execute(op);
        }
        public Task ReplaceAsync(T entity, bool checkConcurrency = false)
        {
            var op = getReplaceOperation(entity, checkConcurrency);
            return CloudTableContext.ExecuteAsync(op);
        }

        public void BulkReplace(IEnumerable<T> entities, bool checkConcurrency = false)
        {
            Bulk(op => op.Replace, entities, checkConcurrency);
        }
        public Task BulkReplaceAsync(IEnumerable<T> entities, bool checkConcurrency = false)
        {
            return BulkAsync(op => op.Replace, entities, checkConcurrency);
        }

        private static readonly bool _isExpandableEntity = typeof(T).IsSubclassOf(typeof(ExpandableTableEntity));
        static TableOperation getMergeOperation(string partitionKey, string rowKey, Action<DynamicTableEntity> action, bool createIfNotExists = false)
        {
            var entity = new DynamicTableEntity
            {
                PartitionKey = partitionKey,
                RowKey = rowKey,
                ETag = "*",
            };
            action(entity);
            if (_isExpandableEntity)
                ExpandableTableEntity.ExpandDictionary(entity.Properties, true);
            return createIfNotExists ? TableOperation.InsertOrMerge(entity) : TableOperation.Merge(entity);
        }
        public void Merge(string partitionKey, string rowKey, Action<DynamicTableEntity> action, bool createIfNotExists = false)
        {
            var op = getMergeOperation(partitionKey, rowKey, action, createIfNotExists);
            CloudTableContext.Execute(op);
        }
        public Task MergeAsync(string partitionKey, string rowKey, Action<DynamicTableEntity> action, bool createIfNotExists = false)
        {
            var op = getMergeOperation(partitionKey, rowKey, action, createIfNotExists);
            return CloudTableContext.ExecuteAsync(op);
        }

        public void BulkMerge(string partitionKey, Action<DynamicTableEntity> action, bool createIfNotExists = false)
        {
            var query = from item in Query()
                        where item.PartitionKey == partitionKey
                        select item.RowKey;
            var entities = query.AsEnumerable()
                .Select(rowKey => new DynamicTableEntity
                {
                    PartitionKey = partitionKey,
                    RowKey = rowKey
                })
                .ToList();

            Bulk(getMergeOperations(action, createIfNotExists), entities);
        }
        public Task BulkMergeAsync(string partitionKey, Action<DynamicTableEntity> action, bool createIfNotExists = false)
        {
            var query = from item in Query()
                        where item.PartitionKey == partitionKey
                        select item.RowKey;
            var entities = query.AsEnumerable()
                .Select(rowKey => new DynamicTableEntity
                {
                    PartitionKey = partitionKey,
                    RowKey = rowKey
                })
                .ToList();

            return BulkAsync(getMergeOperations(action, createIfNotExists), entities);
        }
        static Func<TableBatchOperation, Action<DynamicTableEntity>> getMergeOperations(Action<DynamicTableEntity> action, bool createIfNotExists)
        {
            return op => item =>
            {
                action(item);
                if (_isExpandableEntity)
                    ExpandableTableEntity.ExpandDictionary(item.Properties, true);
                if (createIfNotExists)
                    op.InsertOrMerge(item);
                else
                    op.Merge(item);
            };
        }
    }
}