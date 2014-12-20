using System;
using Microsoft.WindowsAzure.Storage.Blob;

namespace AzureStorageExtensions
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
    public class SettingAttribute : Attribute, ICloneable
    {
        public static readonly SettingAttribute Default = new SettingAttribute();

        public string Name { get; set; }
        public Period Period { get; set; }
        public int RemoveAfter { get; set; }
        public BlobContainerPublicAccessType BlobAccessType { get; set; }

        public object Clone()
        {
            return this.MemberwiseClone();
        }
    }
}
