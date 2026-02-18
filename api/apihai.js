const express = require("express");

const app = express();
app.use(express.json());

// In-memory storage
let batchQueue = [];
let batchCounter = 0;

// -------------------------
// Health Check (Render needs this)
// -------------------------
app.get("/", (req, res) => {
  res.json({ status: "API is running" });
});

// -------------------------
// 1. INGEST ENDPOINT
// -------------------------
app.post("/ingest", (req, res) => {
  const payload = req.body;

  if (!payload.records || !Array.isArray(payload.records)) {
    return res.status(400).json({
      error: "Missing or invalid 'records' field",
    });
  }

  batchCounter += 1;

  const batch = {
    batch_id: batchCounter,
    record_count: payload.records.length,
    records: payload.records,
  };

  batchQueue.push(batch);

  res.json({
    status: "accepted",
    batch_id: batch.batch_id,
    queue_size: batchQueue.length,
  });
});

// -------------------------
// 2. RETRIEVE NEXT BATCH
// -------------------------
app.get("/data", (req, res) => {
  if (batchQueue.length === 0) {
    return res.json({
      status: "empty",
      message: "No batches available",
    });
  }

  const batch = batchQueue.shift();

  res.json({
    status: "success",
    released_batch_id: batch.batch_id,
    record_count: batch.record_count,
    records: batch.records,
    remaining_batches: batchQueue.length,
  });
});

// -------------------------
// Render uses dynamic PORT
// -------------------------
const PORT = process.env.PORT || 10000;

app.listen(PORT, "0.0.0.0", () => {
  console.log(`Server running on port ${PORT}`);
});
