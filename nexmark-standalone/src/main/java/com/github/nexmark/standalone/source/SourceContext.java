package com.github.nexmark.standalone.source;

import java.io.IOException;
import java.util.ArrayList;
import java.io.FileWriter;

import com.fasterxml.jackson.databind.ObjectMapper;

/** Newly added class to mimic the functionality of SourceContext from Flink */
public class SourceContext<T> {
    // Stores all of the items that are a part of the SourceContext instance
    private ArrayList<T> items;

    // Currently tracks whether it is open and generating events
    // Based off of the implementation of SourceContext in Flink
    public boolean isRunning;

    public SourceContext() {
        // Initializing items to be an empty ArrayList (as nothing has been collected/generated yet)
        this.items = new ArrayList<>();

        // Initializing that it is now running
        this.isRunning = true;
    }

    // Adds an item to the items attribute
    public void collect(T item) {
        this.items.add(item);
    }

    // Returns the items (since items is a private attribute)
    public ArrayList<T> getItems() {
        return this.items;
    }

    // Returns the size of the items attribute 
    public int getSize() {
        return this.items.size();
    }

    // This ends the generation of new events
    public void cancel() {
		this.isRunning = false;
	}

    // Adds all elements in items to jsonStrings (except now formatted as json strings)
    public ArrayList<String> jsonFormat() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        ArrayList<String> jsonStrings = new ArrayList<>();

        for(T item : this.items) {
            // Creating JSON string from items collected
            String json = objectMapper.writeValueAsString(item);

            // Splitting to get the nested information about the event/object 
            String[] JSONInfo = json.split("\\{");

            // Either be Person, Auction, or Bid
            String type = JSONInfo[0];

            // Stores information that is unique to each object
            String objInfo = JSONInfo[1];
            String[] objData = objInfo.split(",");

            // Store unique information
            String eventType = "", result = "";

            // Determining type and then formatting the JSON string based on type
            // Done as populated vs null attributes differ based on type
            if (type.contains("Person")) {
                eventType = "0";
                
                // Creating the JSON formatted String
                String personData = "\"id\":" + (objData[0].split("="))[1] + ",\"name\":\"" + (objData[1].split("="))[1] + "\",\"emailAddress\":\"" + (objData[2].split("="))[1] + "\",\"creditCard\":\"" + (objData[3].split("="))[1] + "\",\"city\":\"" + (objData[4].split("="))[1] +"\",\"state\":\"" + (objData[5].split("="))[1] + "\",\"dateTime\":\"" + (objData[6].split("="))[1] + "\",\"extra\":\"" + (objData[7].split("="))[1].split("\\}")[0] + "\"}";
                result = "{\"event_type\":" + eventType + ",\"person\":{" + personData + ",\"auction\":null,\"bid\":null,\"dateTime\":\"" + (objData[6].split("="))[1] + "\"}";
            } else if (type.contains("Bid")) {
                eventType = "2";

                String bidData = "\"auction\":\"" + (objData[0].split("="))[1] + "\",\"bidder\":\"" + (objData[1].split("="))[1] + "\",\"price\":" + (objData[2].split("="))[1] + ",\"channel\":\"" + (objData[3].split("="))[1] + "\",\"url\":\"" + (objData[4].split("="))[1] + "\",\"dateTime\":\"" + (objData[5].split("="))[1] + "\",\"extra\":\"" + (objData[6].split("="))[1].split("\\}")[0] + "\"}";
                result = "{\"event_type\":" + eventType + ",\"person\":null,\"auction\":null,\"bid\":{" + bidData + ",\"dateTime\":\"" + (objData[5].split("="))[1] + "\"}";
            } else {
                eventType = "1";
                
                String auctionData = "\"id\":" + (objData[0].split("="))[1] + ",\"itemName\":\"" + (objData[1].split("="))[1] + "\",\"description\":\"" + (objData[2].split("="))[1] + "\",\"initialBid\":" + (objData[3].split("="))[1] + ",\"reserve\":" + (objData[4].split("="))[1] + ",\"dateTime\":\"" + (objData[5].split("="))[1] + "\",\"expires\":\"" + (objData[6].split("="))[1] + "\",\"seller\":" + (objData[7].split("="))[1] + ",\"category\":" + (objData[8].split("="))[1] + ",\"extra\":\"" + (objData[9].split("="))[1].split("\\}")[0] + "\"}";
                result = "{\"event_type\":" + eventType + ",\"person\":null,\"auction\":{" + auctionData + ",\"bid\":null,\"dateTime\":\"" + (objData[5].split("="))[1] + "\"}";
            }

            jsonStrings.add(result);
        }

        return jsonStrings;
    }

    /** Writes event objects to JSON file 
     * Should a local copy be wanted, this method can be called
    */
    public void writeJson() throws Exception {
        // Opening JSON file to append to (append so don't overwrite already existing data in it)
        FileWriter fw = new FileWriter("jsonData.json", true);

        /** Example String:
        * "Person{id=1002, name='Sarah Smith', emailAddress='lylwz@ezsc.com', creditCard='8658 7331 1113 8728', city='Kent', state='WA', dateTime=2023-06-22T16:13:58.768Z, extra='nvavtdR^IOQTcympjsozradqW`__^YTUUSUaaaljqzN\\JZWVezgffeoygrgpxtwgfwwcabpziiqyhsfjtgrsPLRIPTsrkujcfzfvqicorgiuasnjxg`\\SVSLOMQIQWJ^JL]WPX_TYTYNaXNKwgo'}"
        */
        for (String jsonStr : jsonFormat()) {

            // Appending JSON formatted String to file
            fw.append(jsonStr);
            // Appending new line for clarity between events and improved readability
            fw.append("\n");
        }

        // Closing FileWriter
        fw.close();
    }
}
