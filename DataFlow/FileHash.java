package DataFlow;

import java.util.Map;

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

// Todo: make this a longer hash key, to reduce collisions.
// Look at http://hashids.org/, which probably has a much better scheme.

public class FileHash {
    // Always return Integers.  HashMaps can't handle primitive types.
    static String metaDbHashCode(Map<String, Object> metadata) {
        // Make a big string with all relevant fields.
        String longString = "" + metadata.get("Longitude") +
                metadata.get("Latitude") +
                metadata.get("GPS_Altitude") +
                metadata.get("CreationDate") +
                metadata.get("Make") +
                metadata.get("Model") +
                metadata.get("Image_Description");

        // Let java generate a hashcode for that string.
        // Todo: make sure this is architecture independent.
        // Todo: We are using java to make a 32 bit hash, and then turning that into
        // a string for comparison. This is because MapDB converts Integers into strings anyway.
        // We should make longer strings to reduce collisions.
        // Todo: This can also use type and JPEG, etc.
        return ((Integer) (longString.hashCode())).toString();
    }


}
