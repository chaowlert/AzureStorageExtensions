AzureStorageExtensions
======================

Extensions for Azure Storage Client library

####1. Context
Create Context is easy, just inherit from `BaseCloudContext` and declear property any types of `CloudQueue`, `CloudBlobContainer`, `CloudTable`, and new generic `CloudTable<T>`
```
public class MyContext : BaseCloudContext
{
    public CloudQueue MyQueue { get; set; }
    public CloudBlobContainer MyBlob { get; set; }
    public CloudTable<MyClass> MyTable { get; set; }
}
```
In config file, add ConnectionString match to name of context
```
  <connectionStrings>
    <add name="MyContext" connectionString="{azure storage connection string}" />
  </connectionStrings>
```

####2. CloudTable<T>
The new generic CloudTable allow you to create, retrieve, update, replace, merge, delete, query, and bulk opertations much easier.
```
var person = context.Person[partitionKey, rowKey];
context.Persons.Insert(person, replaceIfExists: true);
context.Persons.Delete(person);

var children = from p in context.Persons.Query()
               where p.Age <= 18
               select p;
```

####3. Archive Table
Table (also blob container and queue) can be parition by time. This is suitable for log or temporary data. Just add setting attribute to property in context.
```
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

####4. Expandable Table Entity
Azure table limits each property to 64k, but ExpandableTableEntity allows you to keep data up to 1M limit.
```
public class Log : ExpandableTableEntity 
{
    public string Message { get; set; } //this message can be up to 1M
}
```
