using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Blob.Protocol;

public static class CloudBlobExtensions
{
    public static void SafeDelete(this CloudBlockBlob blob)
    {
        try
        {
            blob.Delete();
        }
        catch (StorageException ex)
        {
            if (ex.RequestInformation.HttpStatusCode != 404)
                throw;
            if (ex.RequestInformation.ExtendedErrorInformation == null || ex.RequestInformation.ExtendedErrorInformation.ErrorCode == BlobErrorCodeStrings.BlobNotFound)
                return;
            throw;
        }
    }

}
