require('dotenv').config();
const { processUnprocessedPosts } = require('../src/processor');

exports.handler = async function (event, context) {
  try {
    console.log('Starting scheduled audio processing job...');
    
    // Process unprocessed posts
    await processUnprocessedPosts();
    
    return {
      statusCode: 200,
      body: JSON.stringify({
        message: 'Processing completed successfully'
      })
    };
  } catch (error) {
    console.error('Error in scheduled function:', error);
    
    return {
      statusCode: 500,
      body: JSON.stringify({
        error: 'Internal server error',
        details: error.message
      })
    };
  }
}; 