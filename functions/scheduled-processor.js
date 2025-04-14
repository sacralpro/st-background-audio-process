// Simplified version without node-appwrite
require('dotenv').config();
const axios = require('axios');

exports.handler = async function (event, context) {
  try {
    console.log('Starting simplified scheduled audio processing job...');
    
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
    
    // Find posts that need processing
    const queryUrl = `${APPWRITE_ENDPOINT}/databases/${APPWRITE_DATABASE_ID}/collections/${APPWRITE_COLLECTION_ID_POST}/documents`;
    
    const queryResponse = await axios.get(queryUrl, {
      headers,
      params: {
        // Using Appwrite REST API to query
        // Find posts with audio_url but without mp3_url
        'queries[]': [
          'notEqual("audio_url", null)',
          'equal("mp3_url", null)'
        ]
      }
    });
    
    const posts = queryResponse.data.documents || [];
    
    if (posts.length === 0) {
      console.log('No unprocessed posts found.');
      return {
        statusCode: 200,
        body: JSON.stringify({ message: 'No unprocessed posts found' })
      };
    }
    
    console.log(`Found ${posts.length} unprocessed posts.`);
    
    // Call Appwrite function for each post
    const results = [];
    for (const post of posts) {
      try {
        console.log(`Processing post: ${post.$id}`);
        
        // Call Appwrite function to process this post
        const executionUrl = `${APPWRITE_ENDPOINT}/functions/${APPWRITE_FUNCTION_ID}/executions`;
        const payload = { postId: post.$id };
        
        const functionResponse = await axios.post(executionUrl, payload, { headers });
        
        console.log(`Function execution started for post ${post.$id}`);
        results.push({
          postId: post.$id,
          status: 'processing_initiated'
        });
      } catch (error) {
        console.error(`Error initiating processing for post ${post.$id}:`, error.message);
        results.push({
          postId: post.$id,
          status: 'error',
          message: error.message
        });
      }
    }
    
    return {
      statusCode: 200,
      body: JSON.stringify({
        message: `Initiated processing for ${posts.length} posts`,
        results
      })
    };
  } catch (error) {
    console.error('Error in simplified processing function:', error);
    
    return {
      statusCode: 500,
      body: JSON.stringify({
        error: 'Internal server error',
        details: error.message
      })
    };
  }
}; 