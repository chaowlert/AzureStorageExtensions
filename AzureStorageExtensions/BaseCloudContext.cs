using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureStorageExtensions
{
    public abstract class BaseCloudContext
    {
        public CloudClient CloudClient { get; }

        protected BaseCloudContext()
        {
            CloudClient = CloudClient.Get(this.GetType().Name);
            InitializeProperties();
        }
        protected BaseCloudContext(string connectionName)
        {
            CloudClient = CloudClient.Get(connectionName);
            InitializeProperties();
        }

        static readonly ConcurrentDictionary<Type, Action<BaseCloudContext>> _cache = new ConcurrentDictionary<Type, Action<BaseCloudContext>>(); 
        void InitializeProperties()
        {
            var action = _cache.GetOrAdd(this.GetType(), GenerateInitializePropertyAction);
            action(this);
        }
        static Action<BaseCloudContext> GenerateInitializePropertyAction(Type type)
        {
            var cloudContextType = typeof(BaseCloudContext);
            var cloudClientProp = cloudContextType.GetProperty("CloudClient");

            var cloudClientType = typeof(CloudClient);
            var getTableMethod = cloudClientType.GetMethod("GetCloudTable");
            var getQueueMethod = cloudClientType.GetMethod("GetCloudQueue");
            var getBlobMethod = cloudClientType.GetMethod("GetCloudBlobContainer");
            var getGenericTableMethod = cloudClientType.GetMethod("GetGenericCloudTable");

            var p = Expression.Parameter(cloudContextType, "p");
            var propList = new List<Expression>();

            var cloudClient = Expression.Variable(cloudClientType, "cloudClient");
            propList.Add(Expression.Assign(cloudClient, Expression.Property(p, cloudClientProp)));

            var context = Expression.Variable(type, "context");
            propList.Add(Expression.Assign(context, Expression.Convert(p, type)));

            foreach (var prop in type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.GetProperty | BindingFlags.SetProperty))
            {
                MethodInfo method;
                if (prop.PropertyType == typeof(CloudTable))
                    method = getTableMethod;
                else if (prop.PropertyType == typeof(CloudQueue))
                    method = getQueueMethod;
                else if (prop.PropertyType == typeof(CloudBlobContainer))
                    method = getBlobMethod;
                else if (prop.PropertyType.IsGenericType && prop.PropertyType.GetGenericTypeDefinition() == typeof(CloudTable<>))
                {
                    method = getGenericTableMethod.MakeGenericMethod(prop.PropertyType.GetGenericArguments()[0]);
                }
                else
                    continue;
                var setting = prop.GetCustomAttribute<SettingAttribute>() ?? SettingAttribute.Default;
                setting = (SettingAttribute)setting.Clone();
                if (string.IsNullOrEmpty(setting.Name))
                    setting.Name = prop.Name;
                if (prop.PropertyType == typeof(CloudQueue) || prop.PropertyType == typeof(CloudBlobContainer))
                    setting.Name = setting.Name.ToLower();
                var key = type.FullName + "." + prop.Name;
                var call = Expression.Call(cloudClient, method, Expression.Constant(key), Expression.Constant(setting));
                var exp = Expression.Assign(Expression.Property(context, prop), call);
                propList.Add(exp);
            }
            var body = Expression.Block(new[] {cloudClient, context}, propList);
            return Expression.Lambda<Action<BaseCloudContext>>(body, p).Compile();
        }
    }
}
