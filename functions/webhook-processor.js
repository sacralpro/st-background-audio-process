require('dotenv').config();
const { processPost } = require('../src/processor');
const { getDatabases, Query } = require('node-appwrite');

exports.handler = async function (event, context) {
  // Ensure we're receiving a POST request
  if (event.httpMethod !== 'POST') {
    return {
      statusCode: 405,
      body: JSON.stringify({ error: 'Method Not Allowed' })
    };
  }

  try {
    // Parse the webhook payload
    const payload = JSON.parse(event.body);
    
    // Verify this is a database event for post collection
    if (
      !payload.event ||
      !payload.event.includes('databases') ||
      !payload.event.includes('collections') ||
      !payload.event.includes('documents') ||
      !payload.payload ||
      !payload.payload.$id
    ) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: 'Invalid webhook payload' })
      };
    }
    
    // Check if this is a document create/update event
    const isCreateOrUpdate = payload.event.includes('create') || payload.event.includes('update');
    
    if (!isCreateOrUpdate) {
      return {
        statusCode: 200,
        body: JSON.stringify({ message: 'Skipping non-create/update event' })
      };
    }
    
    // Get the document details
    const documentId = payload.payload.$id;
    
    // Connect to Appwrite and get the full document
    const databases = getDatabases();
    const document = await databases.getDocument(
      process.env.APPWRITE_DATABASE_ID,
      process.env.APPWRITE_COLLECTION_ID_POST,
      documentId
    );
    
    // Check if this post has an unprocessed WAV file
    if (document.audio_url && !document.mp3_url) {
      console.log(`Processing new post from webhook: ${documentId}`);
      
      // Process the post
      await processPost(document);
      
      return {
        statusCode: 200,
        body: JSON.stringify({ message: 'Post processing started' })
      };
    }
    
    return {
      statusCode: 200,
      body: JSON.stringify({ message: 'No processing needed for this post' })
    };
  } catch (error) {
    console.error('Error processing webhook:', error);
    
    return {
      statusCode: 500,
      body: JSON.stringify({ error: 'Internal server error', details: error.message })
    };
  }
}; 