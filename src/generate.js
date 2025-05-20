import fs from "fs";

// Function to generate a random number in a range
function getRandomInRange(min, max) {
  return (Math.random() * (max - min) + min).toFixed(4);
}

// Function to format timestamp by adding seconds
function addSecondsToTimestamp(baseTimestamp, seconds) {
  const date = new Date(baseTimestamp);
  date.setSeconds(date.getSeconds() + seconds);
  return date.toISOString();
}

// Generate 10,000 synthetic records
const NUM_RECORDS = 10000;
const data = [];
const baseMMSI = 100000000; // Starting MMSI
const baseLat = 40.7128; // Starting latitude (around New York)
const baseLon = -74.0060; // Starting longitude
const baseTimestamp = "2025-05-15T16:10:00Z";

for (let i = 0; i < NUM_RECORDS; i++) {
  const mmsi = (baseMMSI + i).toString().padStart(9, "0"); // 9-digit MMSI
  const lat = (baseLat + (i * 0.0001) + parseFloat(getRandomInRange(-0.005, 0.005))).toFixed(4); // Small variation in latitude
  const lon = (baseLon + (i * 0.0001) + parseFloat(getRandomInRange(-0.005, 0.005))).toFixed(4); // Small variation in longitude
  const sog = getRandomInRange(10, 20); // Random speed between 10 and 20 knots
  const timestamp = addSecondsToTimestamp(baseTimestamp, i); // Increment timestamp by 1 second

  data.push({
    MMSI: mmsi,
    LAT: lat,
    LON: lon,
    SOG: sog,
    BaseDateTime: timestamp,
  });
}

// CSV header
const header = "MMSI,LAT,LON,SOG,BaseDateTime\n";
const rows = data.map(row => `${row.MMSI},${row.LAT},${row.LON},${row.SOG},${row.BaseDateTime}`).join("\n");
fs.writeFileSync("C://Users//Minfy//Desktop//XOVIAN-MAIN//Valkey-POC//valkey-strems//data//test_vessel_movements.csv", header + rows);
console.log(`Generated data/test_vessel_movements.csv with ${NUM_RECORDS} records`);