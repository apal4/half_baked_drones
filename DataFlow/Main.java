package DataFlow;

import com.cloudant.client.api.Database;
import com.cloudant.client.api.views.Key;
import com.cloudant.client.api.views.ViewRequest;
import com.cloudant.client.api.views.ViewRequestBuilder;
import com.cloudant.client.api.views.ViewResponse;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.IOException;
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
 */

public class Main {
    public static void main(String args[]) throws IOException {

        System.out.println("DataFlow() lib");

        Config config = new Config();
        CloudDb cloudDb = new CloudDb(config);
        Database db = cloudDb.database();

        //get a ViewRequestBuilder from the database for the chosen view
        ViewRequestBuilder viewBuilder = db.getViewRequestBuilder("Views", "test0");
        ViewRequest<String, String> request = viewBuilder.newRequest(Key.Type.STRING, String.class)
                .keys("15dee1d71bf71a30fe9a196f6932743a")
                .build();
        //perform the request and get the response
        ViewResponse<String, String> response = request.getResponse();



        ViewRequestBuilder viewBuilder1 = db.getViewRequestBuilder("Views", "coords1");
        ViewRequest<Number, String> request1 = viewBuilder1.newRequest(Key.Type.NUMBER, String.class)
                .startKey(30.00)
                .endKey(40.00)
                .build();
        ViewResponse<Number, String> response1 = request1.getResponse();


        List< String > list1 = cloudDb.viewQueryNumberStringRange("Views", "coords1", 30.0, 40.0);

        List< Number > list2 = cloudDb.viewQueryNumberNumberRange ("Views", "testNumNum", 30.0, 40.0);
        System.out.println(list2);

        List< Number > list3 = cloudDb.viewQueryStringNumberRange ("Views", "testStringNum", "Madison", "Pop");
        System.out.println(list3);

        List< String  > list4 = cloudDb.viewQueryStringStringRange ("Views", "testStringString", "Madison", "Madison");
        System.out.println(list4);

        List<JsonArray> list5 = cloudDb.viewQueryStringJsonArrayRange ("Views", "testStringList", "Madison", "Madison");
        System.out.println(list5);
        for (JsonArray jsonArray : list5)
        for(JsonElement jsonElement : jsonArray) {
            System.out.println(jsonElement.getAsString());
        }


        ViewRequestBuilder viewBuilder6 = db.getViewRequestBuilder("Views", "testStringDoc");
        ViewRequest<String, JsonObject> request6 = viewBuilder6.newRequest(Key.Type.STRING, JsonObject.class)
                .startKey("Madison")
                .endKey("Madison")
                .build();
        ViewResponse<String, JsonObject> response6 = request6.getResponse();

        System.out.println(response6);
        List<JsonObject> result6 = new ArrayList<JsonObject>();
        for(ViewResponse.Row<String, JsonObject> row : response6.getRows())
            result6.add(row.getValue());

        System.out.println(result6);

        List<JsonObject> list6 = cloudDb. viewQueryStringJsonObjectRange("Views", "testStringDoc", "Madison", "Madison");


         System.out.println(list6);


        // Look here : http://static.javadoc.io/com.cloudant/cloudant-client/2.0.0/com/cloudant/client/api/views/package-summary.html
        // JsonObject jsonObject = cloudDb.dbQuery("Views", "coords1", 39.5841201639006);

        // ViewResponse response = cloudDb.dbQuery("Views", "coords1", 39.5841201639006);
                /*

                view("Views", "coords1")
                .newRequest(Key.Type.STRING, String.class)
                .includeDocs(true)
                .build()
                .getResponse();
                */

    }
}
