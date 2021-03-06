﻿using System;
using Microsoft.Azure.Storage.Blob;

namespace AzureStorageExtensions
{
    [AttributeUsage(AttributeTargets.Property)]
    public class SettingAttribute : Attribute, ICloneable
    {
        public static readonly SettingAttribute Default = new SettingAttribute();

        public string Name { get; set; }
        public Period Period { get; set; }
        public int RemoveAfter { get; set; }
        public BlobContainerPublicAccessType BlobAccessType { get; set; }

        public object Clone()
        {
            return MemberwiseClone();
        }
    }
}