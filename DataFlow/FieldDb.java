package DataFlow;

import com.google.gson.Gson;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.util.*;
import java.util.concurrent.ConcurrentMap;

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

// The hashCodes used here should be perfect - that is, each
// file should generate a unique hashcode.
// Currently only store image MetaData objects, but this could be extended.
public class FieldDb {
    // Currently based on MapDB.
    private DB mDb;
    private ConcurrentMap mDbMap = null;
    // The file where the database is stored.
    private String mDBPathname = "/tmp/FieldDb0.db";

    // todo: Currently only one DB is supported (but not enforced).
    // todo: we never close the local database.
    // todo: delete below.  We've ended this one.
    /*
    public FieldDb() {
        // Open the local database.
        // This should be based on the Config file.
        mDb = DBMaker.fileDB(mDBPathname).closeOnJvmShutdown().make();
        mDbMap = mDb.hashMap("map").createOrOpen();
    }
    */

    public FieldDb(Config config){
        mDb = DBMaker.fileDB(config.fieldDbPathname ()).closeOnJvmShutdown().make();
        mDbMap = mDb.hashMap("map").createOrOpen();
    }
    public FieldDb(String pathname){
        mDb = DBMaker.fileDB(pathname).closeOnJvmShutdown().make();
        mDbMap = mDb.hashMap("map").createOrOpen();
    }
    public void close(){
        if(!mDb.isClosed()) {
            mDb.close();
        }
    }

    /** Writes a MetaData to the metaData base.
     *
     * @param metaData The MetaData to be written.
     */
    public void write(MetaData metaData) {
        // todo: This should check to see if object already exists in DB.
        if (metaData != null) {
            // Get a map with the metadata from the file's header.
            Map<String, Object> map = metaData.map();
            if (map != null) {
                // Turn the doc into Json, and put it in the local database,
                // using the metadata hash as the key.
                Gson gson = new Gson();
                String json = gson.toJson(map);
                if (json != null) {
                    System.out.println(json);
                    String hashKey = (String)map.get("MetaDbHashCode");
                    System.out.println("hashKey = " + hashKey);
                    // Todo: null pointer exceptions here can corrupt the
                    // mDbMap. Explore.
                    // The issue was that mDbMap was null due to a blunder.
                    // This corrupted that actual database.

                    mDbMap.put(hashKey, json);
                    // Todo: ensure atomiticity here.
                } else System.out.println("json is null");
            } else System.out.println("map is null");
        } else System.out.println("metaData is null");
    }

    /** Returns a MetaData object corresponding to the hash code.
     * Returns null if there is no corresponding MetaData in the field DB.
     * @param fieldHash The hashcode unique to the file.
     */

    MetaData read(int fieldHash) {
        String document = (String) mDbMap.get(fieldHash);
        if(document == null) return null;
        MetaData result = new MetaData(document);
        return result;
    }

    /** Returns the metadata object in this that
     * corresponds to the metadata object.
     * This is the "official" metadata.
     * @param metaData
     * @return
     */
    MetaData get(MetaData metaData){
        return read( metaData.fieldHash());
    }

    /**
     * Returns true if the MetaData is in the database. False otherwise.
     */
    public boolean find(MetaData metaData) {
        // Get the hash for the argument.
        int fieldHash =  metaData.fieldHash();
        // Look up the object in the table.
        MetaData result = read(fieldHash);
        // See if anything was in the table.
        if (result == null) return false; // Nothing in table? No match.

        // Otherwise, call the function that compares the
        // fields of the two objects.
        // Note that the _id's can be different, and both objects are considered equal();
        // todo : The fieldHash's should be perfect, and therefore there is no need for
        // the equal() check.  It would make more sense to use this
        // as an assert test.
        return (result.equals(metaData));
    }

    /**
     * Returns a list of all fieldHashes in the entire database.
     */
    public List<Integer> fieldHashList() {
        // Get a list of all keys.
        Set<Object> list = mDbMap.keySet();

        // Make the list we will return.
        List<Integer> result = new ArrayList<Integer>();

        // Iterate over the entire keySet, converting each String into an Integer,
        // and adding that integer to the result.
        Iterator itr = list.iterator();
        while (itr.hasNext()) {
            Object string = itr.next();
            result.add(Integer.parseInt( string.toString()));
        }
        return result;
    }

    /** Removes an entry (or entries) from the dB. */
    public void remove(int fieldHash){
        //
        Integer integer = (Integer)fieldHash;
        String key = integer.toString();
        mDbMap.remove(key);

    }
    /** Removes everything from the DB. For testing and development.*/
    // todo: make this a general utility, and everytime we sync we
    // should flush, and then copy entire DB from CloudDB.
    public void clear(){
        mDbMap.clear();
    }

    boolean sync(CloudDb cloudDb) {
        // Get a map of all ImageMetaData in the cloud.
        // In this map the key is a string with the fieldHash.
        // The value is the Metadata itself.
        Map<String, MetaData> cloudMap = cloudDb.fieldDbMap();

        // Iterate over all image data in this,
        // writing everything that that the cloud doesn't have into the cloud.
        for (Object metaDataString : mDbMap.values()) {
            MetaData metaData = new MetaData(metaDataString.toString() );
            String id = metaData.id();
            if (cloudMap.get(id) == null) {
                // cloud hasn't heard about this object yet.
                cloudDb.write(metaData);
            }
        }
        // Now do the reverse.
        // Look at everything returned from the cloud,
        // and copy it into this, as needed.
        for (String fieldHash : cloudMap.keySet()) {
            MetaData metaData = cloudMap.get(fieldHash);
            if (!find(metaData))
                write(metaData);
        }

        return true;
    }

    /** Returns the number of Image MetaData objects in this. */
    public int
    imageMetaDataCount(){
        // This database has only image metadata, so we just return the size of the map.
        return mDbMap.size();
    }

    /** Changes this to exactly reflect contents of cloudDB.
     *  This is not a sync, since it does not update
     *  the cloud in any way.
     *  For development and testing.
     * @param cloudDb
     */
    public void  copyFromCloud(CloudDb cloudDb) {
        // Get rid of anything we might have in our database.
        mDbMap.clear();
        // An now sync, since we have nothing to write to the cloud.
        // Todo: this could be more efficient and straightforward.
        // todo:  we could have one function that copied cloud => field.
        sync(cloudDb);

    }

}