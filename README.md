AzureStorageExtensions
======================

Extensions for Azure Storage Client library

[![NuGet](https://img.shields.io/nuget/v/AzureStorageExtensions.svg)](https://www.nuget.org/packages/AzureStorageExtensions)

### Get it
```
PM> Install-Package AzureStorageExtensions
```

### Basic usage

#### 1. Context
Create Context is easy, just inherit from `BaseCloudContext` and declear property any types of `CloudQueue`, `CloudBlobContainer`, `CloudTable`, and new generic `CloudTable<T>`
```csharp
public class MyContext : BaseCloudContext
{
    public MyContext(string connectionString) : base(connectionString) { }

    public CloudQueue MyQueue { get; set; }
    public CloudBlobContainer MyBlob { get; set; }
    public CloudTable<MyClass> MyTable { get; set; }
}
```

#### 2. CloudTable<T>
The new generic CloudTable allow you to create, retrieve, update, replace, merge, delete, query, and bulk opertations much easier.

```csharp
var person = await context.Person.RetrieveAsync(partitionKey, rowKey);
await context.Persons.InsertAsync(newPerson);
await context.Persons.InsertOrReplaceAsync(newPerson);
await context.Persons.InsertOrMergeAsync(partialPerson);
await context.Persons.UpdateAsync(person);
await context.Persons.ReplaceAsync(person);
await context.Persons.MergeAsync(partialPerson);
await context.Persons.DeleteAsync(person);

//bulk also support for all operations
await context.Persons.BulkInsertAsync(persons);

//async query also support (required System.Linq.Async)
var children = await (from p in context.Persons.Query()
                      where p.Age <= 18
                      select p)
               .AsAsyncTableQuery()
               .ToListAsync();
```

#### 3. Archive Table
Table (also blob container and queue) can be parition by time. This is suitable for log or temporary data. Just add setting attribute to property in context.

```csharp
public class MyContext : BaseCloudContext
{
    [Setting(Period=Period.Month)]
    public CloudTable<Log> Logs { get; set; }
}
```
When new logs insert to table it will be kept based on time.  
`Log201410`  
`Log201411`  
`Log201412`  

#### 4. Expandable Table Entity
Azure table limits each property to 64k, but ExpandableTableEntity allows you to keep data up to 1M limit.

```csharp
public class Log : ExpandableTableEntity 
{
    public string Message { get; set; } //this message can be up to 1M
}
```

### Migrate from 1.x

`AzureStorageExtensions` is no more depending on `Web.Config`. You can add constructor and read connection string.

```csharp
public class MyContext : BaseCloudContext
{
    public MyContext() 
    : base(ConfigurationManager.ConnectionStrings[nameof(MyContext)].ConnectionString) { }
}
```