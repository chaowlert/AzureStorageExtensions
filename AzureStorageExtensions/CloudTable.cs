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
            CloudTableContext = cloudTable;
        }

        public CloudTable CloudTableContext { get; }

        public T this[string partitionKey, string rowKey] => single(partitionKey, rowKey);

        public void Insert(T entity, bool replaceIfExists = false)
        {
            var op = replaceIfExists ? TableOperation.InsertOrReplace(entity) : TableOperation.Insert(entity);
            CloudTableContext.Execute(op);
        }

        void innerBulk<U>(TableBatchOperation op, Action<U> action, IEnumerable<U> entities, bool checkConcurrency = false) where U : class, ITableEntity
        {
            foreach (var item in entities)
            {
                if (!checkConcurrency)
                    item.ETag = "*";
                action(item);
                if (op.Count < 100)
                    continue;
                CloudTableContext.ExecuteBatch(op);
                op.Clear();
            }
        }

        internal void Bulk<U>(Func<TableBatchOperation, Action<U>> func, IEnumerable<U> entities, bool checkConcurrency = false, Action<TableBatchOperation> afterBulk = null) where U : class, ITableEntity
        {
            var op = new TableBatchOperation();
            var action = func(op);
            innerBulk(op, action, entities, checkConcurrency);
            afterBulk?.Invoke(op);
            if (op.Count > 0)
                CloudTableContext.ExecuteBatch(op);
        }

        public void BulkInsert(IEnumerable<T> entities, bool replaceIfExists = false)
        {
            Bulk(op => replaceIfExists ? new Action<T>(op.InsertOrReplace) : op.Insert, entities);
        }

        public TableQuery<T> Query()
        {
            return CloudTableContext.CreateQuery<T>();
        }

        T single(string partitionKey, string rowKey)
        {
            var op = TableOperation.Retrieve<T>(partitionKey, rowKey);
            var result = CloudTableContext.Execute(op);
            return (T)result.Result;
        }

        U retry<U>(Func<U> func)
        {
            var policy = CloudTableContext.ServiceClient.DefaultRequestOptions.RetryPolicy;
            var retry = 0;
            while (true)
            {
                try
                {
                    return func();
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

        public T Update(string partitionKey, string rowKey, Action<T> action, bool createIfNotExists = false)
        {
            T result = null;
            CheckUpdate(partitionKey, rowKey, obj =>
            {
                action(obj);
                result = obj;
                return true;
            }, createIfNotExists);
            return result;
        }

        public bool CheckUpdate(string partitionKey, string rowKey, Func<T, bool> func, bool createIfNotExists = false)
        {
            return retry(() =>
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
                            $"Error update: Not Found\nPartitionKey: {partitionKey}\nRowKey: {rowKey}",
                            null);
                    }
                }
                if (!func(obj))
                    return false;

                var op = isNew ? TableOperation.Insert(obj) : TableOperation.Replace(obj);
                CloudTableContext.Execute(op);
                return true;
            });
        }

        void bulkApply(Dictionary<string, T> dict, IEnumerable<T> updated)
        {
            Bulk(op => item =>
            {
                T value;
                if (dict.TryGetValue(item.RowKey, out value))
                {
                    dict.Remove(item.RowKey);
                    if (value.ApplyTo(item))
                        op.InsertOrReplace(item);
                }
                else
                    op.InsertOrReplace(item);
            }, updated, true, op => innerBulk(op, op.Delete, dict.Values));
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
            retry(() =>
            {
                var dict = (from item in Query()
                            where item.PartitionKey == partitionKey
                            select item).ToDictionary(item => item.RowKey);
                var updated = func(partitionKey, dict);

                Bulk(op => item =>
                {
                    if (dict.ContainsKey(item.RowKey))
                        op.Replace(item);
                    else
                        op.Insert(item);
                }, updated.Values, true);
                return 0;
            });
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
            retry(() =>
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

                Bulk(op => item =>
                {
                    if (dict.ContainsKey(item.RowKey))
                        op.Replace(item);
                    else
                        op.Insert(item);
                }, updated.Values, true);
                return 0;
            });
        }

        public void Delete(T entity)
        {
            entity.ETag = "*";
            var op = TableOperation.Delete(entity);
            CloudTableContext.Execute(op);
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
            CloudTableContext.Execute(op);
        }

        public void BulkDelete(IEnumerable<T> entities)
        {
            Bulk(op => op.Delete, entities);
        }

        public void BulkDelete(string partitionKey, Expression<Func<T, bool>> predicate)
        {
            var linq = (from item in Query()
                        where item.PartitionKey == partitionKey
                        select item);
            linq = linq.Where(predicate);
            var entities = linq.ToList();

            BulkDelete(entities);
        }

        public void Replace(T entity, bool checkConcurrency = false)
        {
            if (!checkConcurrency)  
                entity.ETag = "*";
            var op = TableOperation.Replace(entity);
            CloudTableContext.Execute(op);
        }

        public void BulkReplace(IEnumerable<T> entities, bool checkConcurrency = false)
        {
            Bulk(op => op.Replace, entities, checkConcurrency);
        }

        private static readonly bool _isExpandableEntity = typeof (T).IsSubclassOf(typeof (ExpandableTableEntity));
        public void Merge(string partitionKey, string rowKey, Action<DynamicTableEntity> action, bool createIfNotExists = false)
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
            var op = createIfNotExists ? TableOperation.InsertOrMerge(entity) : TableOperation.Merge(entity);
            CloudTableContext.Execute(op);
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

            Bulk(op => item =>
            {
                action(item);
                if (_isExpandableEntity)
                    ExpandableTableEntity.ExpandDictionary(item.Properties, true);
                if (createIfNotExists)
                    op.InsertOrMerge(item);
                else
                    op.Merge(item);
            }, entities);
        }
    }
}