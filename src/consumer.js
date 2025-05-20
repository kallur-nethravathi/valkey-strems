import { MongoClient } from "mongodb";
import { valkey, STREAM_KEY, GROUP_NAME, CONSUMER_NAME, ensureConsumerGroup } from "./valkeyClient.js";

async function runConsumer() {
  let mongoClient;
  try {
    // Capture start time
    const startTime = Date.now();
    const startDate = new Date().toISOString();
    console.log(`Starting consumer at: ${startDate}`);

    console.log("Starting consumer...");

    // Connect to MongoDB
    mongoClient = new MongoClient("mongodb://localhost:27017");
    await mongoClient.connect();
    console.log("Connected to MongoDB");
    const db = mongoClient.db("valkey-steams");
    const collection = db.collection("movements");

    // Ensure the consumer group exists
    await ensureConsumerGroup();

    // Start reading from the stream
    console.log(`Consumer ${CONSUMER_NAME} starting to read from stream...`);
    let hasProcessedEntries = false; // Flag to track if we've processed any entries
    let streamingCompleted = false; // Flag to track if streaming phase has completed

    while (true) {
      // Read new entries for the consumer group
      const entries = await valkey.xreadgroup(
        "GROUP", GROUP_NAME, CONSUMER_NAME,
        "BLOCK", 5000, // Wait up to 5 seconds for new entries
        "COUNT", 100,  // Process 100 entries at a time
        "STREAMS", STREAM_KEY, ">"
      );

      if (!entries || entries.length === 0) {
        console.log("No new entries, waiting...");

        // If we've processed entries and this is the first time we have no new entries,
        // mark the streaming phase as completed
        if (hasProcessedEntries && !streamingCompleted) {
          const streamingEndTime = Date.now();
          const streamingDurationMs = streamingEndTime - startTime;
          const minutes = Math.floor(streamingDurationMs / 1000 / 60);
          const seconds = Math.floor((streamingDurationMs / 1000) % 60);
          console.log(`Streaming phase completed at: ${new Date().toISOString()}`);
          console.log(`Streaming phase took ${minutes} minutes and ${seconds} seconds`);
          streamingCompleted = true;
        }

        continue;
      }

      // If we have entries, set the flag to true
      hasProcessedEntries = true;
      streamingCompleted = false; // Reset in case new entries arrive later

      // Process each entry
      const streamEntries = entries[0][1];
      for (const entry of streamEntries) {
        const id = entry[0];
        const fields = entry[1];
        console.log(fields);

        // Transform the entry into a MongoDB document
        const fieldObj = {};
        for (let i = 0; i < fields.length; i += 2) {
          fieldObj[fields[i]] = fields[i + 1];
        }

        // Validate the entry
        if (!fieldObj.vessel_id || !fieldObj.lat || !fieldObj.long || !fieldObj.speed || !fieldObj.timestamp) {
          console.log("Skipping invalid entry:", fieldObj);
          await valkey.xack(STREAM_KEY, GROUP_NAME, id);
          continue;
        }

        const movement = {
          stream_id: id,
          vessel_id: fieldObj.vessel_id,
          lat: parseFloat(fieldObj.lat),
          long: parseFloat(fieldObj.long),
          speed: parseFloat(fieldObj.speed),
          timestamp: fieldObj.timestamp,
        };

        // Additional validation: ensure numeric fields are valid
        if (isNaN(movement.lat) || isNaN(movement.long) || isNaN(movement.speed)) {
          console.log("Skipping entry with invalid numeric fields:", movement);
          await valkey.xack(STREAM_KEY, GROUP_NAME, id);
          continue;
        }

        // Save to MongoDB
        await collection.insertOne(movement);
        console.log(`Saved movement for vessel ${movement.vessel_id} to MongoDB`);
        await valkey.xack(STREAM_KEY, GROUP_NAME, id);
      }
    }
  } catch (error) {
    console.error(`Consumer error: ${error.message}`);
  } finally {
    // Capture end time and calculate total duration
    const endTime = Date.now();
    const endDate = new Date().toISOString();
    const totalDurationMs = endTime - startTime;
    const totalMinutes = Math.floor(totalDurationMs / 1000 / 60);
    const totalSeconds = Math.floor((totalDurationMs / 1000) % 60);

    console.log(`Consumer completed at: ${endDate}`);
    console.log(`Total runtime: ${totalMinutes} minutes and ${totalSeconds} seconds`);

    if (mongoClient) await mongoClient.close();
    await valkey.quit();
  }
}

runConsumer();