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

import com.cloudant.client.api.ClientBuilder;
import com.cloudant.client.api.CloudantClient;
import com.cloudant.client.api.Database;
import com.cloudant.client.api.views.Key;
import com.cloudant.client.api.views.ViewRequest;
import com.cloudant.client.api.views.ViewRequestBuilder;
import com.cloudant.client.api.views.ViewResponse;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.commons.io.IOUtils;
import org.ektorp.CouchDbInstance;
import org.ektorp.ViewQuery;
import org.ektorp.ViewResult;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// import org.ektorp.CouchDbConnector;

/**
 *  Interface to a CouchDB database, storing
 *  global data for a single enterprise.
 *  Based on Ektorp.  (ektorp.org)
 */

public class CloudDb {

    //  HttpClient mHttpClient;  // The connection to a web server somewhere on the internet.
    CouchDbInstance mDbInstance;  // Manage the connection a Couch server.
    //  private  CouchDbConnector mDb;         // The database on that server.

    // The connection to a web server somewhere on the internet.
    private static CloudantClient mClient;
    private static Database mDb;

    public Database database(){return mDb;}


    /**
     * Constructs a CloudDB.
     */
    /*
    CloudDb() {
        // Todo: currently driven off of properties file.
        // Should use single config file.

        mHttpClient = new StdHttpClient.Builder().build();
        mDbInstance = new StdCouchDbInstance(mHttpClient);
        // if the second parameter is true, the database will be created if it doesn't exists
        mDb = mDbInstance.createConnector("db0", true);
    }
    */

    CloudDb(Config config) {
    // todo: test this.  revised but untested.
        // Copy all DB configuration info from the Config object.
        mClient = ClientBuilder.account(config.cloudDbUsername())
                .username(config.cloudDbUsername())
                .password(config.cloudDbPassword())
                .build();

        // *** mDbInstance = new StdCouchDbInstance(mHttpClient);
        // if the second parameter is true, the database will be created if it doesn't exist.
        mDb = mClient.database(config.cloudDbDatabase(), false);
    }

    /**
     * Writes a single MetaData to the Cloud database.
     */
    public void write(MetaData metaData) {

        try {
            // Extract a map with all fields of metaData,
            // and use that to update the field.
            mDb.update(metaData.map());
        }
        // This will catch the exception thrown if we incorrectly update a MetaData.
        // This should never happen.
        catch (org.ektorp.UpdateConflictException e) {
            System.out.println("UpdateConflict :" + e);
        }
    }

    /**
     * Returns true if the specified hash is present in the Cloud database.
     */
    public boolean isHashPresent(int hashCode) throws IOException {
        return isPresent("Views", "MetaDbHashCode", hashCode);
    }

    /**
     * returns true if the specified query returns any documents.
     */
    public boolean isPresent(String designDoc, String viewName, Object key) throws IOException {
        // Build and execute query.  The curl for this should look like:
        //http://127.0.0.1:5984/db0/_design/<designDoc>/_view/<viewName>:key=<key>

        JsonObject queryResult = dbQuery(designDoc, viewName, key);

        // todo: if (queryResult.getTotalRows() > 0) return true; else return false;
        return true;
    }

    /**
     * Returns a list of the metaData found by the given query.
     *
     * @param designDoc
     * @param viewName
     * @param key
     * @return
     */
    public List<MetaData> metaDataList(String designDoc, String viewName, Object key) throws IOException {

        // Build and execute query.  The curl for this should look like:
        //http://127.0.0.1:5984/db0/_design/<designDoc>/_view/<viewName>:key=<key>

        JsonObject queryResult = dbQuery(designDoc, viewName, key);

        // The list we will return.
        List<MetaData> result = new ArrayList<MetaData>();

        // Iterate over all of the rows returned by the query.
        // Extract a string with the document, and then
        // create a Metadata from that string. Add the new MetaData
        // to the result.
        /* todo:
        for (ViewResult.Row row : queryResult.getRows()) {
            JsonNode jsonNode = row.getValueAsNode();
            String string = jsonNode.toString();
            result.add(new MetaData(string));
        }
        */
        return result;
    }

    /**
     * Performs a CouchDB query, returning the unprocessed result.
     * The appropriate Couch string  for this should look like:
     *   http://127.0.0.1:5984/db0/_design/<designDoc>/_view/<viewName>:key=<key>
     *
     * @param designDoc - The CouchDB document containing the code for the view.
     * @param viewName - The view that we will query.
     * @param key - The key for the query.
     * @return An Ektorp query object with the results of the query.
     */
    /*
    private ViewResult dbQuery(String designDoc, String viewName, Object key) {
        // Build a query.  The curl for this should look like:
        //http://127.0.0.1:5984/db0/_design/<designDoc>/_view/<viewName>:key=<key>
        ViewQuery query = new ViewQuery()
                .designDocId("_design/" + designDoc)
                .viewName(viewName)
                .key(key);
        // todo: me first ****
        // Make the query to the db, and return the result.
        // ViewResult queryResult = mDb.queryView(query);
        // return queryResult;
        return null;
    }
    */
    /** Returns a list of Strings, using the specified view. */
    List<String> viewQueryNumberStringRange(String designDoc, String viewName, Number start, Number end) {
        // Build and execute the request.
        ViewRequestBuilder viewBuilder1 = mDb.getViewRequestBuilder(designDoc, viewName);
        ViewRequest<Number, String> request1 = viewBuilder1.newRequest(Key.Type.NUMBER, String.class)
                .startKey(start)
                .endKey(end)
                .build();

        ViewResponse<Number, String> response1 = null;
        try {
            response1 = request1.getResponse();
        } catch (IOException e) {
            e.printStackTrace();
        }

        List<ViewResponse.Row<Number, String>> rows = response1.getRows();

        List<String> result = new ArrayList<String>();
        for (ViewResponse.Row<Number, String> row : rows) {
            System.out.print(row);
            result.add(row.getValue());
        }
        return result;
    }


    /** Returns a list of Numbers, searching over a range of numbers,  using the specified view. */
    List<Number> viewQueryNumberNumberRange(String designDoc, String viewName, Number start, Number end) {
        // Build and execute the request.
        ViewRequestBuilder viewBuilder1 = mDb.getViewRequestBuilder(designDoc, viewName);
        // The first type is the type of the key, the second is the type of the value.
        ViewRequest<Number, Number> request1 = viewBuilder1.newRequest(Key.Type.NUMBER, Number.class)
                .startKey(start)
                .endKey(end)
                .build();

        ViewResponse<Number, Number> response1 = null;
        try {
            response1 = request1.getResponse();
        } catch (IOException e) {
            e.printStackTrace();
        }

        List<ViewResponse.Row<Number, Number>> rows = response1.getRows();

        List<Number> result = new ArrayList<Number>();
        for (ViewResponse.Row<Number, Number> row : rows) {
            System.out.print(row);
            result.add(row.getValue());
        }
        return result;
    }


    /** Returns a list of Number, searching over a range of Strings,  using the specified view. */
    List<Number> viewQueryStringNumberRange(String designDoc, String viewName, String start, String end) {
        // Build and execute the request.
        ViewRequestBuilder viewBuilder = mDb.getViewRequestBuilder(designDoc, viewName);
        // The first type is the type of the key, the second is the type of the value.
        ViewRequest<String , Number> request = viewBuilder.newRequest(Key.Type.STRING, Number.class)
                .startKey(start)
                .endKey(end)
                .build();

        ViewResponse<String, Number> response = null;
        try {
            response = request.getResponse();
        } catch (IOException e) {
            e.printStackTrace();
        }

        List<ViewResponse.Row<String, Number>> rows = response.getRows();

        List<Number> result = new ArrayList<Number>();
        for (ViewResponse.Row<String, Number> row : rows) {
            System.out.print(row);
            result.add(row.getValue());
        }
        return result;
    }


    /** Returns a list of Strings, searching over a range of Strings,  using the specified view. */
    List<String> viewQueryStringStringRange(String designDoc, String viewName, String start, String end) {
        // Build and execute the request.
        ViewRequestBuilder viewBuilder = mDb.getViewRequestBuilder(designDoc, viewName);
        // The first type is the type of the key, the second is the type of the value.
        ViewRequest<String , String> request = viewBuilder.newRequest(Key.Type.STRING, String.class)
                .startKey(start)
                .endKey(end)
                .build();

        ViewResponse<String, String> response = null;
        try {
            response = request.getResponse();
        } catch (IOException e) {
            e.printStackTrace();
        }

        List<ViewResponse.Row<String, String>> rows = response.getRows();

        List<String> result = new ArrayList<String>();
        for (ViewResponse.Row<String, String> row : rows) {
            System.out.print(row);
            result.add(row.getValue());
        }
        return result;
    }

    /** Returns a list of Strings, searching over a range of Strings,  using the specified view. */
    List<JsonArray> viewQueryStringJsonArrayRange(String designDoc, String viewName, String start, String end) {
        // Build and execute the request.
        ViewRequestBuilder viewBuilder = mDb.getViewRequestBuilder(designDoc, viewName);
        // The first type is the type of the key, the second is the type of the value.
        ViewRequest<String , JsonArray> request = viewBuilder.newRequest(Key.Type.STRING, JsonArray.class)
                .startKey(start)
                .endKey(end)
                .build();

        ViewResponse<String, JsonArray> response = null;
        try {
            response = request.getResponse();
        } catch (IOException e) {
            e.printStackTrace();
        }

        List<ViewResponse.Row<String, JsonArray>> rows = response.getRows();

        List<JsonArray> result = new ArrayList<JsonArray>();
        for (ViewResponse.Row<String, JsonArray> row : rows) {
            System.out.print(row);
            result.add(row.getValue());
        }
        return result;
    }

    /** Returns a list of JsonObjects, searching over a range of Strings,  using the specified view. */
    List<JsonObject> viewQueryStringJsonObjectRange(String designDoc, String viewName, String start, String end)
    throws IOException
    {
        ViewRequestBuilder viewBuilder6 = mDb.getViewRequestBuilder(designDoc, viewName);
        ViewRequest<String, JsonObject> request6 = viewBuilder6.newRequest(Key.Type.STRING, JsonObject.class)
                .startKey(start)
                .endKey(end)
                .build();
        ViewResponse<String, JsonObject> response6 = request6.getResponse();

        List<JsonObject> result6 = new ArrayList<JsonObject>();
        for (ViewResponse.Row<String, JsonObject> row : response6.getRows())
            result6.add(row.getValue());

        return result6;
    }



    /* Example:
    // Each value is ["foo","bar", ... ]
    List<JsonArray> list = cloudDb.viewQueryStringJsonArrayRange ("Views", "testStringList", "Madison", "Madison");
        System.out.println(list);
        for (JsonArray jsonArray : list)
        for(JsonElement jsonElement : jsonArray) {
            System.out.println(jsonElement.getAsString());
        }
     */




    public JsonObject dbQuery(String designDoc, String viewName, Object key)

            throws IOException {

        //get a ViewRequestBuilder from the database for the chosen view
        ViewRequestBuilder viewBuilder = mDb.getViewRequestBuilder( designDoc, viewName);
        /*
        ViewRequest<String, String> request = viewBuilder.newRequest(Key.Type.STRING, String.class)
                .keys(key)
                .build();

        //perform the request and get the response
        //   ViewResponse<String, String> response = request.getResponse();


        return request.getSingleValue();
        */
        return null;
    }




    /**
     * Performs a CouchDB query, for cases where we want an entire view returned.
     * The curl for this query will look like:
     * //http://127.0.0.1:5984/db0/_design/<designDoc>/_view/<viewName>:key=<key>
     * @param designDoc - The CouchDB document containing the code for the view.
     * @param viewName The view that we will query.
     * @return An Ektorp query object with the results of the query.
     */

    private ViewResult dbQuery(String designDoc, String viewName) {
        // Build a query.  The curl for this should look like:
        //http://127.0.0.1:5984/db0/_design/<designDoc>/_view/<viewName>:key=<key>
        ViewQuery query = new ViewQuery()
                .designDocId("_design/" + designDoc)
                .viewName(viewName);
        // Make the query to the db:
        // Todo:
        // ViewResult queryResult = mDb.queryView(query);
        // return queryResult;
        return null;
    }


    /**
     * Returns a count of the image metadata objects.
     */
    /*  todo:
    public int imageMetaDataCount() {
        // Get the view that sums based on
        // type = "ImageMetaData"
        ViewResult result = dbQuery("Views", "ImageMetaDataCount");

        // Special case when there is are no MetaData.
        if(  result.getSize() == 0) return 0;

        // Get a value from the first row.
        // This will break soon, because it will sum according to
        // fileType, so we know how many JPEGs, MP4s, etc.
        return Integer.parseInt(result.getRows().get(0).getValue());

    }
    */
    /**
     * Returns a MetaData from the database, using the ID
     *
     * @param id - The unique ID for every MetaData.
     * @return MetaData corresponding to the id.
     */
    public MetaData get(String id) throws IOException {


        InputStream inputStream = mDb.find(id);
        String string = IOUtils.toString(inputStream, "UTF-8");
        if (string == null) return null;
        return new MetaData(string);
    }

    /**
     * Returns a list of all IDs of all image MetaData in database.
     */
    List<String> imageMetaDataIDs() {
        List<String> result = new ArrayList<String>();
        ViewResult viewResult = dbQuery("Views", "ImageMetaDataIDs");
        for (ViewResult.Row row : viewResult)
            result.add(row.getKey());
        return result;
    }


    /**
     * Returns of list of all MetaData documents in this.
     * Doesn't work. Needs to be debugged.
     */
    /*
    public List<MetaData> allMetaData() {
        ViewQuery query = new ViewQuery()
                .allDocs();
        ViewResult queryResult = mDb.queryView(query);

        // The list we will return.
        List<MetaData> result = new ArrayList<MetaData>();

        // Iterate over all of the rows returned by the query.
        // Extract a string with the document, and then
        // create a Metadata from that string. Add the new MetaData
        // to the result.
        for (ViewResult.Row row : queryResult.getRows()) {
            // JsonNode jsonNode = row.getValueAsNode();
            String id = row.getId();
            // Run our bullshit test to determine the type of the document.
            if ((id != null) && !(id.equals("_design/Views"))) {

                // Make a string from the row, and then make
                // a MetaData from that str
                // Add the resulting MetaData to the result.
                String doc = row.getDoc();
                MetaData metaData = new MetaData(doc);
                if (metaData != null)
                    result.add(metaData);
            }
        }
        return result;
    }
    */
    /**
     * deletes all entries with negative hashes.
     * For testing, it will delete roughly half the entries in this database.
     */
    public void deleteNegativeMetaData() throws IOException {
        // Get a list of everything in the  metadata view.
        ViewResult allMetaData = dbQuery("Views", "MetaData");

        // Iterate over each row of the result.
        for (ViewResult.Row row : allMetaData.getRows()) {
            // Extract a string with the Json for the Couchdb document.
            String doc = row.getValue();
            // Use that string to make our metadata class.
            MetaData metaData = new MetaData(doc);

            // Look a the field hash, and if its negative, delete it.
            if (metaData != null) {
                int fieldHash = metaData.fieldHash();
                if (fieldHash < 0)
                    delete(metaData.get("_id").toString());
            }

        }
    }

    /**
     * For testing and debugging.
     * Deletes all MetaData from the database.
     */
    /*
    public void deleteAllMetaData() {


        // Build a query with everything in the MetaData view.
        // At this writing this view has everything but the
        // design documents.
        // The curl for this should look like:
        //http://127.0.0.1:5984/db0/_design/Vies/_view/MetaData
        ViewQuery query = new ViewQuery()
                .designDocId("_design/Views")
                .viewName("MetaData");

        // Make the query to the db:
        ViewResult queryResult = mDb.queryView(query);


        for (ViewResult.Row row : queryResult.getRows()) {
            String id = row.getId();  // We need the id and revision number to delete.
            // Make a string with the json, use that to make a JsonObject, wich in turn
            // can be queried for the revision.
            String string = row.getValue();
            JsonObject jsonObject = new Gson().fromJson(string, JsonObject.class);
            JsonElement rev = jsonObject.get("_rev");
            String revString = rev.getAsString();
            // And finally delete the object.
            mDb.delete(id, revString);
        }
    }
    */
    /** Deletes an object from the database. */
    public void delete(String id) throws IOException {
        MetaData metaData = get(id);
        if (metaData != null) {
            String rev = (String) metaData.get("_rev");
            // Todo: find cloudant version.
            // if (rev != null) mDb.delete(id, rev);
        }
    }

        /*
        ViewResult queryResult = dbQuery("Views", "by_id", id);
        if (queryResult.getRows().size() > 0) {
            ViewResult.Row row = queryResult.getRows().get(0);
            String string = row.getValue();
            JsonObject jsonObject = new Gson().fromJson(string, JsonObject.class);
            JsonElement rev = jsonObject.get("_rev");
            String revString = rev.getAsString();

            mDb.delete(id, revString);
        }
        */


    /**
     * Returns the value of a single field given an id.
     * Returns false if there is no such id, or no such field.
     * Untested.
     */
    JsonObject fieldValue(String id, String fieldName) throws IOException {

        InputStream inputStream = mDb.find(id);
        String string = IOUtils.toString(inputStream, "UTF-8");
        JsonObject jsonObject = new Gson().fromJson(string, JsonObject.class);

        // Make sure that a row was returned.
        return jsonObject.get(fieldName).getAsJsonObject();

    }

    /**
     * Returns a map with all ImageMetaData in the Cloud DB.
     * The keys are the hashStrings, and values are MetaData objects.
     * Map format is consistent with FieldDbs.
     */
    public Map<String, MetaData> fieldDbMap() {

        Map<String, MetaData> result = new HashMap<String, MetaData>();
        // Make a query that returns everything in MetaDbHashCode view.
        // This returns the the hashCode as the key, and the document as the value.
        ViewResult queryResult = dbQuery("Views", "MetaDbHashCode");

        for (ViewResult.Row row : queryResult.getRows()) {

            // Get the key from the row, which is the hashString for the metadata.
            String hashString = row.getKey();

            // Get the value from the row, and use it to build
            // a MetaData.
            String valueString = row.getValue();
            MetaData metaData = new MetaData(valueString);

            // And put them into the result.
            result.put(hashString, metaData);

        }
        return result;
    }

    /** Returns a list of the IDs of all files that have not
     * been copied to file storage.
     */
    List<String> filesNotStored() {

        List<String> result = new ArrayList<String>();

        // Make a query to get contents of "FileNotStored" view.
        // This view has id's of all documents where
        //   doc.type == "ImageMetaData" && doc.FileStoreDB == null
        // This currently has key=_id and value = 1;
        ViewResult queryResult = dbQuery("Views", "FilesNotStored");

        // Copy each key into the result.
        for (ViewResult.Row row : queryResult.getRows())
            result.add(row.getKey());

        return result;
    }



    /** Stores all files that haven't been stored already.
     *
     * @param cloudStorage
     * @param sourcePathName = A string giving the directory with Image files
     * @return  The number of files copied to cloud storage.
     */
    int copyToCloudStorage(CloudStorage cloudStorage, String sourcePathName) throws IOException {

        Path sourcePath = Paths.get(sourcePathName);
        int count = 0;

        // Get a list of ids of all files that have metadata in the cloud,
        // but the file itself has not been stored in the cloud.
        List<String> filesNotStored = filesNotStored();

        // Iterate over this list, trying to store each file.
        for (String id : filesNotStored) {
            // The files are found in the field directory under their ids,
            // and we store them to the cloud using that id.
            if (cloudStorage.put(id, sourcePathName  + id)) {
                // Then we succeeded.
                System.out.println("stored " + id + " to Cloud");

                // Get the metadata for this file.
                // Add the name of the cloud storage DB.
                MetaData metaData = get(id);
                metaData.put("fileStoreID", cloudStorage.name());

                // And write the altered metadata back to the cloud database.
                write(metaData);

                count++;
            } else
                System.out.println("failed to store " + id + " to Cloud");
        }

        return count;
    }

    /** Clears all Image MetaData. For testing. */
    void clear() throws IOException {

        // Get a list of all image meta data ids.
        List<String> ids = imageMetaDataIDs() ;
        for(String id:ids)
            delete(id);
    }

}
