require('dotenv').config();
const { processUnprocessedPosts } = require('./src/processor');

// Check environment variables
const requiredEnvVars = [
  'APPWRITE_ENDPOINT',
  'APPWRITE_PROJECT_ID',
  'APPWRITE_API_KEY',
  'APPWRITE_DATABASE_ID',
  'APPWRITE_COLLECTION_ID_POST',
  'APPWRITE_BUCKET_ID'
];

const missingEnvVars = requiredEnvVars.filter(envVar => !process.env[envVar]);

if (missingEnvVars.length > 0) {
  console.error('Missing required environment variables:', missingEnvVars.join(', '));
  process.exit(1);
}

console.log('Starting background audio processor service...');

// Initial run
processUnprocessedPosts();

// Set up interval to check for new unprocessed posts
const CHECK_INTERVAL = parseInt(process.env.CHECK_INTERVAL) || 60000; // Default to 1 minute
setInterval(processUnprocessedPosts, CHECK_INTERVAL);

console.log(`Service running. Checking for new audio files every ${CHECK_INTERVAL / 1000} seconds.`); 