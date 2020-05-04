using System;

namespace AzureStorageExtensions
{
    public interface ILeasable
    {
        DateTime? LeaseExpire { get; set; }
    }
}