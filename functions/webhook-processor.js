require('dotenv').config();
const axios = require('axios');

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
    
    // Get the document ID
    const documentId = payload.payload.$id;
    
    // Configuration
    const {
      APPWRITE_ENDPOINT,
      APPWRITE_PROJECT_ID,
      APPWRITE_API_KEY,
      APPWRITE_DATABASE_ID,
      APPWRITE_COLLECTION_ID_POST,
      APPWRITE_FUNCTION_ID
    } = process.env;
    
    // Setup Appwrite API headers
    const headers = {
      'Content-Type': 'application/json',
      'X-Appwrite-Project': APPWRITE_PROJECT_ID,
      'X-Appwrite-Key': APPWRITE_API_KEY
    };
    
    // Get the document to check if it needs processing
    const documentUrl = `${APPWRITE_ENDPOINT}/databases/${APPWRITE_DATABASE_ID}/collections/${APPWRITE_COLLECTION_ID_POST}/documents/${documentId}`;
    const documentResponse = await axios.get(documentUrl, { headers });
    const document = documentResponse.data;
    
    // Check if this post has an unprocessed WAV file
    if (document.audio_url && !document.mp3_url) {
      console.log(`Processing new post from webhook: ${documentId}`);
      
      // Call Appwrite function to process this post
      const executionUrl = `${APPWRITE_ENDPOINT}/functions/${APPWRITE_FUNCTION_ID}/executions`;
      const functionPayload = { postId: documentId };
      
      await axios.post(executionUrl, functionPayload, { headers });
      
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