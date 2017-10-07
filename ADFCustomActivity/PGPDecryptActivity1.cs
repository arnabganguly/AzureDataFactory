using System.IO;
using System.Globalization;
using System.Diagnostics;
using System.Linq;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Azure.Management.DataFactories.Models;
using Microsoft.Azure.Management.DataFactories.Runtime;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;

using MyDotNetActivityNS.PGP;

namespace MyDotNetActivityNS
{
    public class PGPDecryptActivity1 : IDotNetActivity
    {
        /// <summary>
        /// Execute method is the only method of IDotNetActivity interface you must implement.
        /// In this sample, the method invokes the Calculate method to perform the core logic.  
        /// </summary>
        public IDictionary<string, string> Execute(IEnumerable<LinkedService> linkedServices, IEnumerable<Dataset> datasets, Activity activity, IActivityLogger logger)
        {
            // get extended properties defined in activity JSON definition
            // (for example: SliceStart)
            DotNetActivity dotNetActivity = (DotNetActivity)activity.TypeProperties;         

            // to log information, use the logger object
            // log all extended properties            
            IDictionary<string, string> extendedProperties = dotNetActivity.ExtendedProperties;
            logger.Write("Logging extended properties if any...");
            foreach (KeyValuePair<string, string> entry in extendedProperties)
            {
                logger.Write("<key:{0}> <value:{1}>", entry.Key, entry.Value);
            }

            // linked service for input and output data stores
            // in this example, same storage is used for both input/output
            AzureStorageLinkedService inputLinkedService;

            // get the input dataset
            Dataset inputDataset = datasets.Single(dataset => dataset.Name == activity.Inputs.Single().Name);

            // declare variables to hold type properties of input/output datasets
            AzureBlobDataset inputTypeProperties, outputTypeProperties;

            // get type properties from the dataset object
            inputTypeProperties = inputDataset.Properties.TypeProperties as AzureBlobDataset;

            // log linked services passed in linkedServices parameter
            // you will see two linked services of type: AzureStorage
            // one for input dataset and the other for output dataset 
            foreach (LinkedService ls in linkedServices)
                logger.Write("linkedService.Name {0}", ls.Name);

            // get the first Azure Storate linked service from linkedServices object
            // using First method instead of Single since we are using the same
            // Azure Storage linked service for input and output.
            inputLinkedService = linkedServices.First(
                linkedService =>
                linkedService.Name.ToLower() ==
                "AzureStorageLinkedService".ToLower()).Properties.TypeProperties
                as AzureStorageLinkedService;

            // get the connection string in the linked service
            string connectionString = inputLinkedService.ConnectionString;

            // get the folder path from the input dataset definition
            string folderPath = "pgpfiles/"; // GetFolderPath(inputDataset);
            string output = string.Empty; // for use later.

            // create storage client for input. Pass the connection string.
            CloudStorageAccount inputStorageAccount = CloudStorageAccount.Parse(connectionString);
            CloudBlobClient inputClient = inputStorageAccount.CreateCloudBlobClient();

            // initialize the continuation token before using it in the do-while loop.
            BlobContinuationToken continuationToken = null;
            string outputResult = string.Empty;
            string pgpInputFileName = string.Empty;

            MemoryStream inputStream = new MemoryStream();
            MemoryStream inputKeyStream = new MemoryStream();
            MemoryStream outDecryptedStream = new MemoryStream();
            do
            {   // get the list of input blobs from the input storage client object.
                BlobResultSegment blobList = inputClient.ListBlobsSegmented(folderPath,
                                         true,
                                         BlobListingDetails.Metadata,
                                         null,
                                         continuationToken,
                                         null,
                                         null);

                // Calculate method returns the number of occurrences of
                // the search term (“Microsoft”) in each blob associated
                // with the data slice. definition of the method is shown in the next step.
                //output = Calculate(blobList, logger, folderPath, ref continuationToken, "Microsoft");
                
                logger.Write("number of blobs found: {0}", blobList.Results.Count<IListBlobItem>());
                foreach (IListBlobItem listBlobItem in blobList.Results)
                {
                    CloudBlockBlob inputBlob = listBlobItem as CloudBlockBlob;

                    if (inputBlob != null)
                    {
                        // find pgp source file
                        logger.Write("Blob Uri: {0}", inputBlob.Uri.AbsoluteUri);
                        logger.Write("Blob exists: {0}", inputBlob.Exists());

                        string inputfile = inputBlob.Uri.AbsoluteUri;
                        if (inputfile.Contains("key"))
                        {
                            if (!File.Exists(inputfile))
                                logger.Write("Private Key File [{0}] not found.", inputfile);

                                                   
                            inputBlob.DownloadToStream(inputKeyStream);
                            if(inputKeyStream != null)
                            {
                                logger.Write("Private Key File [{0}] is ready for read.", inputfile);
                            }
                            inputKeyStream.Position = 0;
                        }
                        else if (inputfile.Contains("csv.gz.pgp"))
                        {
                            if (!File.Exists(inputfile))
                                logger.Write("Encrypted File  [{0}] not found.", inputfile);
                                                        
                            inputBlob.DownloadToStream(inputStream);
                            if (inputStream != null)
                            {
                                logger.Write("Encrypted File [{0}] is ready for read.", inputfile);
                            }
                            inputStream.Position = 0;
                            pgpInputFileName = inputfile;
                        }
                    }
                }

            } while (continuationToken != null);

            //set the PGP key
            string pgpPrivateKey = dotNetActivity.ExtendedProperties["pgpPrivateKeyLocation"];  //FvNzRQQMC2pzkpUZ47XT55TSPkUIil
            //set output file name
            string pgpOutputFile = pgpInputFileName.Substring(0, (pgpInputFileName.LastIndexOf(".")));
            //Call the PGP Decrypt method
            Stream outputStream = PGPDecrypt.Decrypt(inputStream, inputKeyStream, pgpPrivateKey);
            
            // get the output dataset using the name of the dataset matched to a name in the Activity output collection.
            Dataset outputDataset = datasets.Single(dataset => dataset.Name == activity.Outputs.Single().Name);

            // get type properties for the output dataset
            outputTypeProperties = outputDataset.Properties.TypeProperties as AzureBlobDataset;

            // get the folder path from the output dataset definition
            folderPath = GetFolderPath(outputDataset);

            // log the output folder path   
            logger.Write("Writing blob to the folder: {0}", folderPath);


            ////// create a storage object for the output blob.
            CloudStorageAccount outputStorageAccount = CloudStorageAccount.Parse(connectionString);
            // write the name of the file.
            Uri outputBlobUri = new Uri(outputStorageAccount.BlobEndpoint, folderPath + pgpOutputFile);// "/" +  GetFileName(outputDataset));

            // log the output file name
            logger.Write("output blob URI: {0}", outputBlobUri.ToString());

            // create a blob and upload the output text.
            CloudBlockBlob outputBlob = new CloudBlockBlob(outputBlobUri, outputStorageAccount.Credentials);
            //logger.Write("Writing {0} to the output blob", output);
            outputBlob.UploadFromStream(outputStream);

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

        /// <summary>
        /// Iterates through each blob (file) in the folder, counts the number of instances of search term in the file,
        /// and prepares the output text that is written to the output blob.
        /// </summary>

        public static string Calculate(BlobResultSegment Bresult, IActivityLogger logger, string folderPath, ref BlobContinuationToken token, string searchTerm)
        {
            string output = string.Empty;
            logger.Write("number of blobs found: {0}", Bresult.Results.Count<IListBlobItem>());
            foreach (IListBlobItem listBlobItem in Bresult.Results)
            {
                CloudBlockBlob inputBlob = listBlobItem as CloudBlockBlob;
                if ((inputBlob != null) && (inputBlob.Name.IndexOf("$$$.$$$") == -1))
                {
                    string blobText = inputBlob.DownloadText(Encoding.ASCII, null, null, null);
                    logger.Write("input blob text: {0}", blobText);
                    string[] source = blobText.Split(new char[] { '.', '?', '!', ' ', ';', ':', ',' }, StringSplitOptions.RemoveEmptyEntries);
                    var matchQuery = from word in source
                                     where word.ToLowerInvariant() == searchTerm.ToLowerInvariant()
                                     select word;
                    int wordCount = matchQuery.Count();
                    output += string.Format("{0} occurrences(s) of the search term \"{1}\" were found in the file {2}.\r\n", wordCount, searchTerm, inputBlob.Name);
                }
            }
            return output;
        }
    }
}
