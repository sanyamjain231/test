const express = require("express");
const AWS = require("aws-sdk");
const mysql = require("mysql");
const bodyParser = require("body-parser");

const app = express();
const port = process.env.PORT || 3000;

// Configure AWS SDK with credentials and region
AWS.config.update({
  accessKeyId: "",
  secretAccessKey: "",
  region: "",
});

const s3 = new AWS.S3();

// Create a MySQL connection pool
const pool = mysql.createPool({
  connectionLimit: 10,
  host: "localhost",
  user: "root",
  password: "root",
  database: "s3_operations",
});

app.use(bodyParser.json());

// Get Object
app.get("/objects/:bucket/:key", async (req, res) => {
  const { bucket, key } = req.params;

  try {
    const data = await s3.getObject({ Bucket: bucket, Key: key }).promise();
    res.json(data.Body.toString("utf-8"));

    // Log the operation in the MySQL database
    await logOperation(bucket, key, "GET");
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

// Put Object
app.post("/objects/:bucket/:key", async (req, res) => {
  const { bucket, key } = req.params;
  const { content } = req.body;

  try {
    await s3.putObject({ Bucket: bucket, Key: key, Body: content }).promise();
    res.json({ message: "Object successfully uploaded" });

    // Log the operation in the MySQL database
    await logOperation(bucket, key, "PUT");
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

// Delete Object
app.delete("/objects/:bucket/:key", async (req, res) => {
  const { bucket, key } = req.params;

  try {
    await s3.deleteObject({ Bucket: bucket, Key: key }).promise();
    res.json({ message: "Object successfully deleted" });

    // Log the operation in the MySQL database
    await logOperation(bucket, key, "DELETE");
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

// List Objects
app.get("/objects/:bucket", async (req, res) => {
  const { bucket } = req.params;

  try {
    const data = await s3.listObjectsV2({ Bucket: bucket }).promise();
    const keys = data.Contents.map((obj) => obj.Key);
    res.json(keys);

    // Log the operation in the MySQL database
    await logOperation(bucket, "", "LIST");
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

// List Buckets
app.get("/buckets", async (req, res) => {
  try {
    const data = await s3.listBuckets().promise();
    const buckets = data.Buckets.map((bucket) => bucket.Name);
    res.json(buckets);

    // Log the operation in the MySQL database
    await logOperation("", "", "LIST BUCKETS");
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

// Function to log S3 operation in the MySQL database
async function logOperation(bucket, key, operation) {
  const query =
    "INSERT INTO s3_operations (bucket, s3_key, operation) VALUES (?, ?, ?)";
  const values = [bucket, key, operation];

  return new Promise((resolve, reject) => {
    pool.query(query, values, (error, results) => {
      if (error) {
        reject(error);
      } else {
        resolve(results);
      }
    });
  });
}

app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});
