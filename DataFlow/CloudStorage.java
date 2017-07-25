package DataFlow;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jkoch on 11/15/16.
 * <p>
 * ***********************************************************************
 * <p>
 * Pilot Training System CONFIDENTIAL
 * __________________
 * <p>
 * [2015] - [2016] Pilot Training System
 * All Rights Reserved.
 * <p>
 * NOTICE:  All information contained herein is, and remains
 * the property of Pilot Training System,
 * The intellectual and technical concepts contained
 * herein are proprietary to Pilot Training System
 * and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Pilot Training System.
 * Manages the storage of files in the cloud.
 * Currently supports only Amazon S3.
 */

public class CloudStorage {

    // The s3 name for the bucket, and the name for the meta database.
    // These should eventually come from the same Config object.
    private  String mBucketName = "pts.bucket1";
    private  String mDbName = "db0";
    private  String mFolderName = "Images/";

    private AmazonS3 mS3Client;


    public String name(){return mDbName;}

    CloudStorage() {
        mS3Client = new AmazonS3Client(new ProfileCredentialsProvider());
    }


    CloudStorage(Config config) {

        // Get the credentials from the configuration object.
        // Todo: implement security.
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(config.awsAccessKeyID(), config.awsSecretAccessKeyID());
        mS3Client = new AmazonS3Client(awsCreds);
        mFolderName = config.awsImageFolderName();
    }

    /**
     * Copies a file from the cloud to pathname.
     * *
     *
     * @param id       - Unique identifier for file.
     * @param pathname - The path to the file on the field computer.
     * @return true if operation succeeds.
     */
    public boolean put(String id, String pathname) {

        try {
            // Make a path to the file we will create.
            File file = new File(pathname);

            String key = mFolderName + id;
            mS3Client.putObject(new PutObjectRequest(mBucketName, key, file));

        } catch (AmazonServiceException ase) {
            System.out.println("Amazon S3 error while copying file : " + ase);
            return false;
        }
        return true;
    }
    /** Returns all Ids. For testing.  Currently limited to 1000 ids.
     * Returns the ID without the Folder in the string.
     * */
    List<String> allIds() {
        // Based on an example found here:
        //  http://docs.aws.amazon.com/AmazonS3/latest/dev/ListingObjectKeysUsingJava.html

        List<String> result = new ArrayList<String>();

        try {

            // Make an S3 object request
            final ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(mBucketName).withPrefix(mFolderName).withMaxKeys(1000);

            ListObjectsV2Result v2result;
            do {
                v2result = mS3Client.listObjectsV2(request);

                for (S3ObjectSummary objectSummary : v2result.getObjectSummaries()) {

                    // Get a string with the complete objectName.
                    // This will look something like "Images/sadrgju-fdghjvcxw"
                    String completeName = objectSummary.getKey();
                    // Strip off the foldername.
                    String id = completeName.replace(mFolderName, "");
                    if (id.length() > 0)  // S3 also gives us the name of Folder.  Don't save this.
                        // Copy the Image ID into the result.
                        result.add(id);
                }

            } while (v2result.isTruncated() == true);

        } catch (AmazonServiceException ase) {
            System.out.println("Amazon S3 error while copying file : " + ase);
        }
        return result;
    }

    /** Deletes an object from the cloud.  For testing. */
    boolean delete(String id) {

        try {
            mS3Client.deleteObject(new DeleteObjectRequest(mBucketName,mFolderName+id));
        }
        catch (AmazonServiceException ase)

        {
            System.out.println("Caught an AmazonServiceException, which " +
                    "means your request made it " +
                    "to Amazon S3, but was rejected with an error response" +
                    " for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
            return false;
        }

        return true;
    }

    /**
     * Deletes all objects that are not found in the Cloud Database.
     * Returns the number of objects deleted.
     * For testing and development.
     * @param cloudDB The cloud database that determines the files to be deleted.
     * @return The number of files deleted.
     */
    int clearFilesNotInCloudDb(CloudDb cloudDb) {

        int count = 0;

        // Get a list of all IDs of ImageMetaData in Cloud DB.
        List<String> cloudIDs = cloudDb.imageMetaDataIDs();

        // And a list of all IDs in storage.
        // We are assuming the storage only holds images,
        // keyed by the ID.
        List<String> storageIDs = allIds();

        // Iterate over all the IDs in cloud storage.
        // If we don't find that ID in the list from Cloud Database,
        // delete it from storage, and increment the count.
        for (String storageID : storageIDs)
            if (!cloudIDs.contains(storageID)) {
                delete(storageID);
                count++;
            }
        return count;
    }

    /**
     * Clear all Image files in cloud file store.
     * For development and testing.
     *
     * @return number of files deleted.
     */
    int clear() {

        int count = 0;
        // And a list of all IDs in storage.
        // We are assuming the storage only holds images,
        // keyed by the ID.
        List<String> storageIDs = allIds();

        for (String storageID : storageIDs) {
            // Todo: check to see if these were really deleted.
            delete(storageID);
            count++;
        }
        return count;
    }




    /**
     * Returns true if the id exists on the file store.
     * Does not examine the file itself.
     *
     * @param id The unique identifier that we check for.
     * @return true if a file exists using that identifier.
     */
    boolean doesIdExist(String id) {
        boolean result = false;

        try {
            result = mS3Client.doesObjectExist(mBucketName, mFolderName + id);
        } catch (AmazonClientException ex) {
            System.out.println("Amazon Client Exception " + ex);
            return false;
        }
        return result;
    }
}



