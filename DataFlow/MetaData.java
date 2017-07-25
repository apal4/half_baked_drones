package DataFlow;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.jpeg.JpegParser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

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
// Todo: rename to ImageMetaData
public  class MetaData {

    Map<String, Object> mMetaMap;

    /**
     * Constructor that uses a a file as input.
     * For now this should be a JPEG.
     *
     * @param file
     */

    Map<String, Object> map() { return mMetaMap;}


    /** Constructs a MetaData from an image file.
     *  Reads all information that comes from the file header.
     *  Then extract the subset that we care about, and renaming some
     *  of the keys
     *  @param file A file containing a JPEG image.
     */
    MetaData(File file) {
        // Read the file header, extracting the information
        // we use to manage the file.
        mMetaMap = readJpegFileHeader(file);
        // Calculate the field hash.  This is based on the meta data we
        // just read, and should be identical only for identical files.
        mMetaMap.put("MetaDbHashCode", FileHash.metaDbHashCode(mMetaMap));
        // Generate the unique id. This will be used as the filename on the
        // field machine, and as the object name in the file store.
        mMetaMap.put("_id", UUID.randomUUID().toString());

        // The type tell us that this is image metadata.
        mMetaMap.put("type", "ImageMetaData");

        // And filetype (roughly corresponding the file extension) is JPEG.
        mMetaMap.put("fileType", "JPEG");

        // And note the name the file.  This name is provided by the drone.
        mMetaMap.put("FileName", file.getName());
    }

    /* Constucts a MetaData from a json string.
    *  @param json string.
    *
    */
    MetaData(String string) {
        // Todo: Should this check that type = ImageMetaData?
        mMetaMap = new HashMap<String, Object>();
        //  Gson gson = new Gson(string);
        //  HashMap<String,String> map = new Gson().fromJson(string, new TypeToken<HashMap<String, String>>(){}.getType());
        HashMap<String, Object> map = new Gson().fromJson(string, new TypeToken<HashMap<String, String>>() {
        }.getType());
        mMetaMap = map;
    }

    /** Returns the unique id that represents the file. */
    public String id(){return (String)mMetaMap.get("_id");}

    public void put(String key, Object value){
        // Todo: Assert that we are never changing the FieldHash, or the _id.
        mMetaMap.put(key, value);
    }

    String fieldHashAsString(){
        if(mMetaMap == null) return null;
        Object object =  mMetaMap.get("MetaDbHashCode");
        if (object == null) return null;  // Error.  All MetaData should have a hash.
        String string = object.toString();
        return string;
    }

    /* Returns the code used to manage objects in the field database.
    * We are always operating on Strings, because they are returned reliably.
    */
    int fieldHash() {
        if(mMetaMap == null) return 0;
        Object object =  mMetaMap.get("MetaDbHashCode");
        if (object == null) return 0;  // Error.  All MetaData should have a hash.
        String string = object.toString();
        Integer result = Integer.parseInt(string);
        // System.out.println("type is " + value.getClass());
        return result;
    }
    // todo:
    // 1. We can't use uuids for this purpose because we want be able to
    //    flag headers that are identical because the drone has a file
    //    that has already been processed.
    //    What we really want is to have this compare headers,
    //    and these hashes are just part of the efficiency hack.
    //    The interface should be
    //       boolean fieldDb::find(MetaData)
    //    and
    //       boolean fieldDB::add(MetaData)
    //


    // Used to translate the names in the EXIF header to our standard names.
    // The values in the table are also the definition of equals() for this class.
    // This is JPEG dependent, and possibly DJI dependent.
    private static final Map<String, String> mRenameTable = new HashMap<String, String>() {{
        put("geo:long", "Longitude");
        put("geo:lat", "Latitude");
        put("GPS Altitude", "GPS_Altitude");
        put("Creation-Date", "CreationDate");
        put("Image Description", "Image_Description");
        put("Make", "Make");
        put("Model", "Model");

    }};


    /**
     * Returns a map with the metadata in jpegFile.
     * Assumes file is valid JPEG.
     * Returns a subset of the metadata found in the file,
     * as determined by mRenameTable above.
     * Other metadata is adding in the constructor that calls this method.
     *
     * @param jpegFile - A jpeg file.
     * @return A map with the contents of the file's metadata header.
     */
    static private Map<String, Object> readJpegFileHeader(File jpegFile) {

        Map<String, Object> result = new HashMap<String, Object>();
        try {
            // Create the map of everything in the header
            // Todo: should this be a HashMap?  Simpler class might be more efficient.
            Map<String, Object> completeHeader = new HashMap<String, Object>();

            // Open the file with the Jpeg.
            FileInputStream inStream = new FileInputStream(jpegFile);

            // Parse the metadata from the file.
            //  Todo: check to see that this is a legitimate JPEG.
            BodyContentHandler handler = new BodyContentHandler();
            Metadata metadata = new Metadata();
            ParseContext pcontext = new ParseContext();
            JpegParser JpegParser = new JpegParser();
            JpegParser.parse(inStream, handler, metadata, pcontext);
            String[] metadataNames = metadata.names();

            // Make the shorter map that we will return.
            // Note that some of the keys are renamed.

            String newKey = null;
            for (String key : metadataNames)
                if ((newKey = mRenameTable.get(key)) != null)
                    result.put(newKey, metadata.get(key));
        } catch (IOException ex) {
            System.out.println("JPEG Exception: " + ex);
        } catch (SAXException ex) {
            System.out.println("JPEG Exception: " + ex);
        } catch (TikaException ex) {
            System.out.println("JPEG Exception: " + ex);
        }

        return result;
    }

    /** Generates the Json representation of Image Metadata. */
    public String toString() {
        String result = "{";

        // This allows us write seperators only between items.
        String previousSeparator = "";
        for (String key : mMetaMap.keySet()) {
            // The first iteration doesn't write a seperator.
            result += previousSeparator + "\"" + key + "\": \"" + mMetaMap.get(key) + "\"";
            previousSeparator = ",\r\n"; // And now create the seperator to be added on next iteration.
        }
        result += "}";
        return result;
    }

    /**
     * Looks up values in the this.
     */
    public Object get(String key) {
        return mMetaMap.get(key);
    }
    // Note that there is no put().  MetaDatas are immutable.

    /**
     * Compares two MetaDatas, returning true if they represent the same file.
     * Note that the _id's of two metadata need not be identical for them both to
     * represent the same file.
     *
     * @param other - Metadata
     * @return true if both instances represent the same file.
     */
    public boolean equals(MetaData other) {


        if (other == null) return false;
        if (other instanceof MetaData) {
            // First, see if the MetaDbHashCodes are different.
            // This will usually flag the differences.
            if (!(this.fieldHash() == other.fieldHash()))
                return false;

            // Compare all fields in the rename table.
            for (String value : mRenameTable.values()) {
                // The values of this table are the keys for MetaData.
                if (!mMetaMap.get(value).equals(other.get(value))) return false;
            }
            return true;
        }
        return false;
    }

    /**
     * We don't currently use this, but it is included to ensure consistency with
     * equals().
     *
     * @return

     @Override
     public int hashCode() {
     // Build string with all the file-dependent fields of this,
     // and then return the java hashCode for this string.
     // Note that this slower than the MetaDbHashCode we store in the object.
     String resultString = "";
     for (String value : mRenameTable.values())
     // The values of this table are the keys for MetaData.
     resultString += mMetaMap.get(value);
     return resultString.hashCode();
     }
     */
}

