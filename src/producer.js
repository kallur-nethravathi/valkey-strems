import fs from "fs";
import csv from "csv-parser";
import { addVesselMovement, valkey } from "./valkeyClient.js";

async function runProducer() {
  try {
    // Capture start time
    const startTime = Date.now();
    const startDate = new Date().toISOString();
    console.log(`Starting producer at: ${startDate}`);
    console.log("Starting producer...");

    let count = 0;
    const stream = fs.createReadStream("C:\\Users\\Minfy\\Desktop\\XOVIAN-MAIN\\Valkey-POC -SINGLE CONSUMER\\valkey-strems\\data\\test_vessel_movements.csv")
      .pipe(csv());

    for await (const movement of stream) {
      // Validate the row
      if (!movement.MMSI || !movement.LAT || !movement.LON || !movement.SOG || !movement.BaseDateTime) {
        console.log("Skipping invalid row:", movement);
        continue;
      }

      // Map CSV fields to expected keys
      const mappedMovement = {
        vessel_id: movement.MMSI,
        lat: parseFloat(movement.LAT),
        long: parseFloat(movement.LON),
        speed: parseFloat(movement.SOG),
        timestamp: movement.BaseDateTime,
      };

      // Additional validation: ensure numeric fields are valid
      if (isNaN(mappedMovement.lat) || isNaN(mappedMovement.long) || isNaN(mappedMovement.speed)) {
        console.log("Skipping row with invalid numeric fields:", mappedMovement);
        continue;
      }

      console.log("@@@@@@@@@", mappedMovement);
      await addVesselMovement(mappedMovement);
      count++;
      if (count % 1000 === 0) {
        console.log(`Processed ${count} movements`);
      }
    }

    console.log(`Finished streaming ${count} movements`);

    // Capture streaming completion time and calculate duration
    const streamingEndTime = Date.now();
    const streamingEndDate = new Date().toISOString();
    const streamingDurationMs = streamingEndTime - startTime;
    const minutes = Math.floor(streamingDurationMs / 1000 / 60);
    const seconds = Math.floor((streamingDurationMs / 1000) % 60);
    console.log(`Streaming completed at: ${streamingEndDate}`);
    console.log(`Streaming took ${minutes} minutes and ${seconds} seconds`);

  } catch (error) {
    console.error(`Producer error: ${error.message}`);
  } finally {
    // Capture total end time and calculate total duration
    const endTime = Date.now();
    const endDate = new Date().toISOString();
    const totalDurationMs = endTime - startTime;
    const totalMinutes = Math.floor(totalDurationMs / 1000 / 60);
    const totalSeconds = Math.floor((totalDurationMs / 1000) % 60);

    console.log(`Producer completed at: ${endDate}`);
    console.log(`Total runtime: ${totalMinutes} minutes and ${totalSeconds} seconds`);

    await valkey.quit();
  }
}

runProducer();