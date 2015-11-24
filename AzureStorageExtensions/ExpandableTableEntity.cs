using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureStorageExtensions
{
    public class ExpandableTableEntity : TableEntity
    {
        public static void ShrinkDictionary(IDictionary<string, EntityProperty> properties)
        {
            foreach (var key in properties.Keys.ToList())
            {
                if (!key.EndsWith(Suffix))
                    continue;
                var lenProp = properties[key];
                if (lenProp.PropertyType != EdmType.Int32)
                    continue;
                var lenValue = lenProp.Int32Value;
                if (lenValue == null || lenValue == 0)
                    continue;
                var len = lenValue.Value;
                var name = key.Substring(0, key.Length - Suffix.Length);
                var first = properties.GetValueOrDefault(name + 0);
                if (first == null)
                    continue;
                if (first.PropertyType == EdmType.Binary)
                {
                    var list = (from i in Enumerable.Range(0, len)
                                select properties.GetValueOrDefault(name + i) into prop
                                where prop != null && prop.PropertyType == EdmType.Binary
                                select prop.BinaryValue).ToArray();
                    if (list.Length != len)
                        continue;
                    var bytes = Extensions.Concat(list);
                    properties.Add(name, new EntityProperty(bytes));
                    properties.Remove(key);
                    for (int i = 0; i < len; i++)
                        properties.Remove(name + i);
                }
                else if (first.PropertyType == EdmType.String)
                {
                    var list = (from i in Enumerable.Range(0, len)
                                select properties.GetValueOrDefault(name + i) into prop
                                where prop != null && prop.PropertyType == EdmType.String
                                select prop.StringValue).ToArray();
                    if (list.Length != len)
                        continue;
                    var str = string.Concat(list);
                    properties.Add(name, new EntityProperty(str));
                    properties.Remove(key);
                    for (int i = 0; i < len; i++)
                        properties.Remove(name + i);
                }
            }            
        }
        const string Suffix = "_Length";
        public override void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            ShrinkDictionary(properties);
            base.ReadEntity(properties, operationContext);
        }

        public static void ExpandDictionary(IDictionary<string, EntityProperty> properties, bool isMerge = false)
        {
            foreach (var key in properties.Keys.ToList())
            {
                var prop = properties[key];
                if (prop.PropertyType == EdmType.Binary)
                {
                    var value = prop.BinaryValue;
                    if (value == null || value.Length <= MaxSize)
                    {
                        properties.Add(key + "_Length", new EntityProperty(isMerge ? 0 : (int?)null));
                        continue;
                    }
                    properties.Remove(key);
                    var len = (value.Length + MaxSize - 1) / MaxSize;
                    properties.Add(key + "_Length", new EntityProperty(len));
                    for (var i = 0; i < value.Length; i += MaxSize)
                    {
                        var size = Math.Min(MaxSize, value.Length - i);
                        var bytes = new byte[size];
                        Buffer.BlockCopy(value, i, bytes, 0, size);
                        properties.Add(key + i, new EntityProperty(bytes));
                    }
                }
                else if (prop.PropertyType == EdmType.String)
                {
                    var value = prop.StringValue;
                    if (value == null || value.Length <= MaxSize/4)
                    {
                        properties.Add(key + "_Length", new EntityProperty(isMerge ? 0 : (int?)null));
                        continue;
                    }
                    var byteCount = Encoding.Unicode.GetByteCount(value);
                    if (byteCount <= MaxSize)
                    {
                        properties.Add(key + "_Length", new EntityProperty(isMerge ? 0 : (int?)null));
                        continue;
                    }
                    properties.Remove(key);
                    var len = 0;
                    var byteStart = 0;
                    var stringStart = 0;
                    var bytes = Encoding.Unicode.GetBytes(value);
                    while (byteStart < bytes.Length)
                    {
                        var count = Math.Min(MaxSize, bytes.Length - byteStart);
                        var stringSize = Encoding.Unicode.GetCharCount(bytes, byteStart, count);
                        var str = value.Substring(stringStart, stringSize);
                        var byteSize = Encoding.Unicode.GetByteCount(str);
                        properties.Add(key + len, new EntityProperty(str));
                        len++;
                        stringStart += stringSize;
                        byteStart += byteSize;
                    }
                    properties.Add(key + "_Length", new EntityProperty(len));
                }
            }            
        }
        const int MaxSize = 65536;
        public override IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            var dict = base.WriteEntity(operationContext);
            ExpandDictionary(dict);
            return dict;
        }
    }
}
