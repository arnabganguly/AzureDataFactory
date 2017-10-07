using System.IO;
using System.Linq;
using System.Collections.Generic;

using Microsoft.Azure.Management.DataFactories.Models;
using Microsoft.Azure.Management.DataFactories.Runtime;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using MyDotNetActivityNS.PGP;

namespace MyDotNetActivityNS
{
    public class PGPDecryptActivity : IDotNetActivity
    {
        /// <summary>
        /// Execute method is the only method of IDotNetActivity interface you must implement.
        /// In this sample, the method invokes the Calculate method to perform the core logic.  
        /// </summary>
        public IDictionary<string, string> Execute(IEnumerable<LinkedService> linkedServices, IEnumerable<Dataset> datasets, Activity activity, IActivityLogger logger)
        {
            DotNetActivity pgpActivity = (DotNetActivity)activity.TypeProperties;
                      
            // get the input dataset
            Dataset inputDataset = datasets.Single(dataset => dataset.Name == activity.Inputs.Single().Name);
            AzureBlobDataset blobDataset = inputDataset.Properties.TypeProperties as AzureBlobDataset;
            logger.Write("\nBlob folder: " + blobDataset.FolderPath);
            logger.Write("\nBlob file: " + blobDataset.FileName);
            string pgpEncyptedFile = blobDataset.FolderPath + "/" + blobDataset.FileName;
            logger.Write("\npgpEncyptedFile file: " + pgpEncyptedFile);
            string pgpOutputFile = blobDataset.FileName.Substring(0, (blobDataset.FileName.LastIndexOf(".")));
            logger.Write("\npgpOutputFile file: " + pgpOutputFile);

            AzureStorageLinkedService inputLinkedService = linkedServices.First(
                    linkedService =>
                    linkedService.Name ==
                    inputDataset.Properties.LinkedServiceName).Properties.TypeProperties
                    as AzureStorageLinkedService;
            
            // get the connection string in the linked service
            string connectionString = inputLinkedService.ConnectionString;

            // create storage client for input. Pass the connection string.
            CloudStorageAccount inputStorageAccount = CloudStorageAccount.Parse(connectionString);
            CloudBlobClient inputClient = inputStorageAccount.CreateCloudBlobClient();

            // find blob to delete and delete if exists.
            Uri blobUri = new Uri(inputStorageAccount.BlobEndpoint, pgpEncyptedFile);
            CloudBlockBlob blob = new CloudBlockBlob(blobUri, inputStorageAccount.Credentials);
            logger.Write("Blob Uri: {0}", blobUri.AbsoluteUri);
            logger.Write("Blob exists: {0}", blob.Exists());

            // string pgpEncyptedFile = pgpActivity.ExtendedProperties["pgpEncyptedFile"]; //"C:\\Users\\admin\\Documents\\FM\\PGP\\fm_aap_loc_20170902.csv.gz.pgp"
            // string pgpOutputFile = pgpActivity.ExtendedProperties["pgpOutputFile"]; //"C:\\Users\\admin\\Documents\\FM\\PGP\\fm_aap_loc_20170902.csv.gz"
            string pgpPrivateKeyLocation = pgpActivity.ExtendedProperties["pgpPrivateKeyLocation"];  //FvNzRQQMC2pzkpUZ47XT55TSPkUIil
            string pgpPrivateKeyPassword = pgpActivity.ExtendedProperties["pgpPrivateKeyPassword"]; //PGP\\cdcvilax023_private.key

            // find blob to delete and delete if exists.
            Uri blobUriPrivateKey = new Uri(inputStorageAccount.BlobEndpoint, blobDataset.FolderPath + "/" + pgpPrivateKeyPassword);
            CloudBlockBlob blobPrivateKey = new CloudBlockBlob(blobUri, inputStorageAccount.Credentials);
            logger.Write("Blob PrivateKey Uri: {0}", blobUriPrivateKey.AbsoluteUri);
            logger.Write("Blob PrivateKey exists: {0}", blobPrivateKey.Exists());
                           
            // get the output dataset using the name of the dataset matched to a name in the Activity output collection.
            Dataset outputDataset = datasets.Single(dataset => dataset.Name == activity.Outputs.Single().Name);

            // get type properties for the output dataset
            AzureBlobDataset outputTypeProperties = outputDataset.Properties.TypeProperties as AzureBlobDataset;

            //// get the folder path from the output dataset definition
            string folderPath = GetFolderPath(outputDataset);

            //PGP decrypt
            //PGPDecrypt.Decrypt("C:\\Users\\admin\\Documents\\FM\\PGP\\fm_aap_loc_20170902.csv.gz.pgp", @"C:\\Users\\admin\\Documents\\FM\\PGP\\cdcvilax023_private.key", "FvNzRQQMC2pzkpUZ47XT55TSPkUIil", "C:\\Users\\admin\\Documents\\FM\\PGP\\abc.gz");
            //PGPDecrypt.Decrypt(pgpEncyptedFile, pgpPrivateKeyLocation, pgpPrivateKeyPassword, pgpOutputFile);
            PGPDecrypt.Decrypt(blobUri.AbsoluteUri, blobUriPrivateKey.AbsoluteUri, pgpPrivateKeyPassword, pgpOutputFile);

            MemoryStream outDecryptedStream = new MemoryStream();
            using (FileStream file = new FileStream(pgpOutputFile, FileMode.Open, FileAccess.Read))
                file.CopyTo(outDecryptedStream);
            
            // create a storage object for the output blob.
            CloudStorageAccount outputStorageAccount = CloudStorageAccount.Parse(connectionString);
            // write the name of the file.
            //Uri outputBlobUri = new Uri(outputStorageAccount.BlobEndpoint, folderPath  + "/" +  GetFileName(outputDataset));
            Uri outputBlobUri = new Uri(outputStorageAccount.BlobEndpoint, pgpOutputFile);

            // log the output file name
            logger.Write("output blob URI: {0}", outputBlobUri.ToString());

            // create a blob and upload the output text.
            CloudBlockBlob outputBlob = new CloudBlockBlob(outputBlobUri, outputStorageAccount.Credentials);
            logger.Write("Writing {0} to the output blob");
            outputBlob.UploadFromStream(outDecryptedStream);

            // The dictionary can be used to chain custom activities together in the future.
            // This feature is not implemented yet, so just return an empty dictionary.  

            return new Dictionary<string, string>();

        }

        /// <summary>
        /// Gets the folderPath value from the input/output dataset.
        /// </summary>

        private static string GetFolderPath(Dataset dataArtifact)
        {
            if (dataArtifact == null || dataArtifact.Properties == null)
            {
                return null;
            }

            // get type properties of the dataset   
            AzureBlobDataset blobDataset = dataArtifact.Properties.TypeProperties as AzureBlobDataset;
            if (blobDataset == null)
            {
                return null;
            }

            // return the folder path found in the type properties
            return blobDataset.FolderPath;
        }

        /// <summary>
        /// Gets the fileName value from the input/output dataset.   
        /// </summary>
        private static string GetFileName(Dataset dataArtifact)
        {
            if (dataArtifact == null || dataArtifact.Properties == null)
            {
                return null;
            }

            // get type properties of the dataset
            AzureBlobDataset blobDataset = dataArtifact.Properties.TypeProperties as AzureBlobDataset;
            if (blobDataset == null)
            {
                return null;
            }

            // return the blob/file name in the type properties
            return blobDataset.FileName;
        }
        
    }
}
