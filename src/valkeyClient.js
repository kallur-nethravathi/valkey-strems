import Redis from "ioredis";

// Initialize Valkey client for local Docker container
const valkey = new Redis({
  host: "localhost",
  port: 6378, // Corrected port
  password: "your-secure-password", // Added password
});

// Stream key for vessel movements
const STREAM_KEY = "vessel:movements:stream";

// Consumer group name
const GROUP_NAME = "vessel-group";
const CONSUMER_NAME = "consumer-1";

// Ensure the consumer group exists
async function ensureConsumerGroup() {
  try {
    await valkey.xgroup("CREATE", STREAM_KEY, GROUP_NAME, "0", "MKSTREAM");
    console.log(`Created consumer group ${GROUP_NAME}`);
  } catch (error) {
    if (!error.message.includes("BUSYGROUP")) {
      console.error(`Error creating consumer group: ${error.message}`);
      throw error;
    }
  }
}

// Add a vessel movement to the stream
async function addVesselMovement(movement) {
  try {
    // Log the movement for debugging
    console.log("[valkeyClient] Adding movement to stream:", movement);
    const entry = await valkey.xadd(
      STREAM_KEY,
      "*",
      "vessel_id", String(movement.vessel_id ?? ""),
      "lat", String(movement.lat ?? ""),
      "long", String(movement.long ?? ""),
      "speed", String(movement.speed ?? ""),
      "timestamp", String(movement.timestamp ?? "")
    );
    return entry;
  } catch (error) {
    console.error(`Error adding to stream: ${error.message}`);
    throw error;
  }
}

export { valkey, STREAM_KEY, GROUP_NAME, CONSUMER_NAME, ensureConsumerGroup, addVesselMovement };