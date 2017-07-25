package DataFlow;

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
 */


public class Config {
    static final String mCloudDbHost = "jkoch.cloudant.com";
    static final String mCloudDbUsername = "jkoch";
    static final String mCloudDbPassword = "Kochie666";
    static final String mCloudDbDatabase = "test4";
    static final String mFieldFileStorePathname = "/tmp/test1/";
    static final String mAwsAccessKeyID = "AKIAJ4I62X3MLP7PC75Q";
    static final String mAwsSecretAccessKeyID = "x+e6vZ94GekY2nSVdsZMvWqZYLXKXNrp89To62mT";
    static final String mAwsImageFolderName = "Images/";
    static final String mFieldDbPathname = "/tmp/FieldDb0.db";


    public String cloudDbHost() {
        return mCloudDbHost;
    }

    public String cloudDbUsername() {
        return mCloudDbUsername;
    }

    public String cloudDbPassword() {
        return mCloudDbPassword;
    }

    public String
    cloudDbDatabase() {
        return mCloudDbDatabase;
    }

    public String fieldFileStorePathname() {
        return mFieldFileStorePathname;
    }

    public String awsAccessKeyID() {
        return mAwsAccessKeyID;
    }

    public String awsSecretAccessKeyID() {
        return mAwsSecretAccessKeyID;
    }

    public String awsImageFolderName() {
        return mAwsImageFolderName;
    }

    public String fieldDbPathname() {
        return mFieldDbPathname;
    }
}

/*
    Description of JSON fields.

    _id     Unique identifier used by CouchDb. Also
            used as filename in field machine, and as
            object name in Cloud Store.

    _rev    Used by CouchDb to manage revisions.

    type            The type of data. ImageMetaData in this program.
    fileType        The type of data stored in file.  Roughly corresponds to
                    file extension.

    CreationDate    The timestamp for file creation from drone metadata.
    Make            The manufacturer of drone. From drone metadata.
    Model           The model of drone.  From drone metadata.
    Longitude       String giving longitude in degrees. From drone metadata.
    Latitude        String giving latitude in degrees. From drone metadata.
    GPS_Altitude    String giving the altitude and units.  From drone metadata.
    Image_Description   Path to image file on drone. From drone metadata.
    MetaDbHashCode  Perfect hash code generated using other metadata.
                    Should be identical if and only if two imagefiles are identical.
    fileStoreID     String identifying the cloud filestore.
                    Not present if the file has not yet been copied to cloud.




 */

/*
All design docs on 9/23/16



{
   "_id": "_design/Views",
   "_rev": "19-f5cf0a591e9975c0ecc79f20814891c9",
   "language": "javascript",
   "views": {
       "MetaDbHashCode": {
           "map": "function(doc) {\n  emit( parseInt(doc.MetaDbHashCode), doc);}"
       },
       "Latitude": {
           "map": "function(doc) {\n  emit( doc.Latitude, doc);}"
       },
       "Longitude": {
           "map": "function(doc) {\n  emit( doc.Longitude, doc);}"
       },
       "MetaData": {
           "map": "function(doc) {\n \tif(doc._id != \"_design/Views\")\n  emit( doc._id, doc);}"
       },
       "by_id": {
           "map": "function(doc) {\n  emit( doc._id, doc);}"
       },
       "ImageMetaDataCount": {
           "map": "function(doc) {\n\nif(doc.type == \"ImageMetaData\")\n  emit(doc.fileType, 1);\n}",
           "reduce": "function(keys, values) {\n    return sum(values);\n}"
       },
       "ImageMetaDataIDs": {
           "map": "function(doc) {\n\nif(doc.type == \"ImageMetaData\")\n  emit(doc._id);\n}"
       },
       "FileStoreDB": {
           "map": "function(doc) {\n\nif(doc.type == \"ImageMetaData\")\n\n  emit(doc._id, doc.FileStoreID);\n}"
       },
       "FileNotStored": {
           "map": "function(doc) {\n\nif(doc.type == \"ImageMetaData\" && \n   doc.fileStoreDB == null)\n  emit(doc._id, 1);\n}",
           "reduce": "function(keys, values) {\n    return sum(values);\n}"
       },
       "FilesNotStored": {
           "map": "function(doc) {\nif(doc.type == \"ImageMetaData\" && \n doc.FileStoreDB == null)\n  emit(doc._id, 1);}\n"
       }
   },
   "unnamed": null
}





 */