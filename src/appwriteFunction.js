// Appwrite Function for audio processing
// Optimized for direct calls from Sacral Track application

const sdk = require('node-appwrite');
const ffmpeg = require('fluent-ffmpeg');
const fs = require('fs');
const path = require('path');
const { promisify } = require('util');
const writeFileAsync = promisify(fs.writeFile);
const readFileAsync = promisify(fs.readFile);
const unlinkAsync = promisify(fs.unlink);
const mkdirAsync = promisify(fs.mkdir);

// Main entry point for Appwrite Function
module.exports = async function(req, res) {
  // Check if this is being called in standard Appwrite Functions manner with single context parameter
  if (req && !res && req.req && req.res) {
    // This looks like it's being called with a single context object as first parameter
    // Adjust our variables to match expected pattern
    const context = req; // First parameter is actually the context
    req = context.req;
    res = context.res;
    console.log('[APPWRITE_FUNCTION] Detected Appwrite context object, adapting parameters');
  }

  // Early check for raw req/res objects to prevent errors
  if (!req || !res) {
    console.log('[APPWRITE_FUNCTION] Warning: req or res is undefined');
    // Create minimal objects to prevent errors
    req = req || {};
    res = res || {
      json: function(data) { return data; }
    };
  }

  // Check if context exists (for Appwrite Function environment)
  const context = { 
    req: req || {}, 
    res: res || {},
    log: function(...args) {
      console.log('[APPWRITE_FUNCTION]', ...args);
    },
    error: function(...args) {
      console.error('[APPWRITE_FUNCTION_ERROR]', ...args);
    }
  };
  
  // Additional diagnostic information
  context.log('Function environment:');
  context.log('- Node version:', process.version);
  context.log('- Environment variables present:', Object.keys(process.env).join(', '));
  context.log('- Appwrite Function ID:', process.env.APPWRITE_FUNCTION_ID || 'Not set');
  context.log('- Appwrite Runtime:', process.env.APPWRITE_RUNTIME || 'Not set');

  // Ensure res has json method
  if (!context.res.json) {
    context.res.json = function(data) {
      return data;
    };
  }
  
  // Dump request info for debugging
  context.log('Request details:');
  if (context.req) {
    context.log('- Method:', context.req.method || 'unknown');
    context.log('- URL:', context.req.url || 'unknown');
    if (context.req.headers) {
      context.log('- Content-Type:', context.req.headers['content-type'] || 'none');
    }
  }

  // Set up Appwrite SDK
  const sdk = require('node-appwrite');
  const fs = require('fs');
  const path = require('path');
  const util = require('util');
  const ffmpeg = require('fluent-ffmpeg');
  
  // Promisify fs functions
  const mkdirAsync = util.promisify(fs.mkdir);
  const writeFileAsync = util.promisify(fs.writeFile);
  const readFileAsync = util.promisify(fs.readFile);
  const unlinkAsync = util.promisify(fs.unlink);
  
  // Initialize Appwrite client
  const client = new sdk.Client();
  
  // Get Appwrite function environment variables
  const { 
    APPWRITE_FUNCTION_PROJECT_ID, 
    APPWRITE_FUNCTION_API_KEY,
    APPWRITE_DATABASE_ID,
    APPWRITE_COLLECTION_ID_POST,
    APPWRITE_BUCKET_ID,
    APPWRITE_ENDPOINT 
  } = process.env;
  
  // Connect to Appwrite
  client
    .setEndpoint(APPWRITE_ENDPOINT || 'https://cloud.appwrite.io/v1')
    .setProject(APPWRITE_FUNCTION_PROJECT_ID)
    .setKey(APPWRITE_FUNCTION_API_KEY);
  
  const databases = new sdk.Databases(client);
  const storage = new sdk.Storage(client);
  
  try {
    // Log the incoming request
    let payload = {};
    
    // Log request info
    if (context.req && context.req.method) {
      context.log(`Received ${context.req.method} request`);
    }
    
    // DEBUG: Log the full raw request body for debugging
    if (context.req && context.req.body) {
      context.log('DEBUG - Full request body:');
      if (typeof context.req.body === 'string') {
        context.log(`"${context.req.body}"`);
      } else {
        context.log(JSON.stringify(context.req.body, null, 2));
      }
      
      // Check if there's a special test flag
      const testMode = context.req.body === 'test' || 
                       (typeof context.req.body === 'string' && context.req.body.includes('test')) ||
                       (context.req.headers && context.req.headers['x-test-mode']);
      
      if (testMode) {
        context.log('TEST MODE DETECTED - Using test postId');
        // Use a test postId - replace with a valid ID from your database
        payload = { postId: '656b66339fa5f91c2b73' };
      }
    }
    
    // Handle potential errors with request body due to invalid content-type
    if (context.req) {
      try {
        // Parse request body if it's a string
        if (context.req.body !== undefined) {
          if (typeof context.req.body === 'string') {
            try {
              // Ensure the string is clean before parsing
              const trimmedBody = context.req.body.trim();
              context.log('Request body string length:', trimmedBody.length);
              
              // For debugging, log the first 100 characters
              if (trimmedBody.length > 0) {
                context.log('First 100 chars of body:', trimmedBody.substring(0, 100));
                
                // Try to parse as JSON
                payload = JSON.parse(trimmedBody);
              }
            } catch (parseError) {
              context.error('Error parsing request body:', parseError.message);
              // Try to use the raw body as a fallback
              context.log('Trying to use raw body as fallback');
              payload = { postId: context.req.body.trim() };
            }
          } else if (typeof context.req.body === 'object') {
            payload = context.req.body;
            context.log('Received request body as object');
          }
        } else if (context.req.rawBody) {
          // If standard body parsing failed, try the rawBody if available
          context.log('Using rawBody as fallback');
          try {
            const rawBody = typeof context.req.rawBody === 'string' 
              ? context.req.rawBody 
              : context.req.rawBody.toString();
            
            const trimmedRawBody = rawBody.trim();
            context.log('Raw body length:', trimmedRawBody.length);
            
            if (trimmedRawBody.length > 0) {
              try {
                payload = JSON.parse(trimmedRawBody);
              } catch (parseError) {
                context.log('Raw body is not JSON, using as postId');
                payload = { postId: trimmedRawBody };
              }
            }
          } catch (rawError) {
            context.error('Error processing raw body:', rawError.message);
          }
        } else if (context.req.bodyRaw) {
          // Another fallback for raw body
          context.log('Using bodyRaw as fallback');
          try {
            const bodyRaw = typeof context.req.bodyRaw === 'string'
              ? context.req.bodyRaw
              : Buffer.isBuffer(context.req.bodyRaw)
                ? context.req.bodyRaw.toString()
                : JSON.stringify(context.req.bodyRaw);
            
            const trimmedBodyRaw = bodyRaw.trim();
            
            if (trimmedBodyRaw.length > 0) {
              try {
                payload = JSON.parse(trimmedBodyRaw);
              } catch (parseError) {
                context.log('bodyRaw is not JSON, using as postId');
                payload = { postId: trimmedBodyRaw };
              }
            }
          } catch (rawError) {
            context.error('Error processing bodyRaw:', rawError.message);
          }
        }
      } catch (bodyError) {
        context.error('Unexpected error processing request body:', bodyError.message);
      }
    } else if (context.req && context.req.headers && context.req.headers['x-appwrite-trigger']) {
      // This might be a webhook or scheduled event from Appwrite
      context.log('Detected Appwrite trigger event');
      
      if (context.req.headers['x-appwrite-trigger'] === 'event') {
        // This is an event trigger
        context.log('Processing event trigger');
        // Try to extract postId from event payload if it exists
        if (context.req.payload && context.req.payload.$id) {
          payload = { postId: context.req.payload.$id };
          context.log(`Extracted postId from event payload: ${payload.postId}`);
        }
      } else if (context.req.headers['x-appwrite-trigger'] === 'schedule') {
        // This is a scheduled trigger
        context.log('Processing scheduled trigger');
        // Try to extract data from scheduled payload
        if (context.req.payload) {
          payload = context.req.payload;
          context.log('Extracted payload from scheduled event');
        }
      }
    }
    
    // Log the payload
    if (Object.keys(payload).length > 0) {
      context.log('Payload:', JSON.stringify(payload).substring(0, 200) + '...');
    } else {
      context.log('Empty payload received');
      payload = {}; // Ensure payload is an object
      
      // Try to find postId in various places as fallback
      if (context.req.query && context.req.query.postId) {
        // Try to get postId from query params
        payload.postId = context.req.query.postId;
        context.log(`Found postId in query params: ${payload.postId}`);
      } else if (context.req.params && context.req.params.postId) {
        // Try to get postId from route params
        payload.postId = context.req.params.postId;
        context.log(`Found postId in route params: ${payload.postId}`);
      } else if (context.req.url) {
        // Try to extract postId from URL parameters
        try {
          const url = new URL(context.req.url);
          const urlParams = new URLSearchParams(url.search);
          if (urlParams.has('postId')) {
            payload.postId = urlParams.get('postId');
            context.log(`Found postId in URL parameters: ${payload.postId}`);
          } else if (url.pathname) {
            // Try to extract from pathname
            const pathParts = url.pathname.split('/');
            const lastPart = pathParts[pathParts.length - 1];
            if (lastPart && lastPart.length > 10) { // Assuming it might be an ID
              payload.postId = lastPart;
              context.log(`Extracted potential postId from URL path: ${payload.postId}`);
            }
          }
        } catch (urlError) {
          context.error('Error parsing URL:', urlError.message);
        }
      } else if (context.req.path) {
        // Try to extract postId from path as last resort
        const pathParts = context.req.path.split('/');
        const lastPart = pathParts[pathParts.length - 1];
        if (lastPart && lastPart.length > 10) { // Assuming it might be an ID
          payload.postId = lastPart;
          context.log(`Extracted potential postId from path: ${payload.postId}`);
        }
      }
    }
    
    // Extract postId from payload
    let postId = payload.postId;
    
    // If still no postId, check if this is an event with post creation
    if (!postId && payload.event && payload.payload) {
      if (payload.event.includes('documents') && 
          (payload.event.includes('create') || payload.event.includes('update'))) {
        postId = payload.payload.$id;
        context.log(`Extracted postId from event payload: ${postId}`);
      }
    }
    
    if (!postId) {
      context.log('No postId found in any request property');
      
      // Last resort: Check if we're in a test/development environment
      if (process.env.NODE_ENV === 'development' || process.env.APPWRITE_FUNCTION_NAME?.includes('dev')) {
        context.log('DEVELOPMENT MODE - Using fallback test postId');
        // Use a test ID in development mode
        postId = '656b66339fa5f91c2b73'; // Replace with a valid ID
      } else {
        // Check if we have direct access to Appwrite event data
        if (req && req.variables && req.variables.$id) {
          postId = req.variables.$id;
          context.log(`Using document ID from Appwrite event variables: ${postId}`);
        } else if (req && req.path && req.path.includes('/databases/')) {
          // Extract ID from Appwrite webhook path pattern
          const parts = req.path.split('/');
          // The pattern might be like /databases/{db}/collections/{coll}/documents/{id}
          for (let i = 0; i < parts.length - 1; i++) {
            if (parts[i] === 'documents' && parts[i+1]) {
              postId = parts[i+1];
              context.log(`Extracted document ID from webhook path: ${postId}`);
              break;
            }
          }
        }
      }
      
      if (!postId) {
        return context.res.json({
          success: false,
          message: 'No postId found in request'
        });
      }
    }
    
    context.log(`Processing audio for post: ${postId}`);
    
    // Get post document
    const post = await databases.getDocument(
      process.env.APPWRITE_DATABASE_ID,
      process.env.APPWRITE_COLLECTION_ID_POST,
      postId
    );
    
    context.log(`Retrieved post: ${post.$id}`);
    
    if (!post.audio_file_id) {
      return context.res.json({
        success: false,
        message: 'No audio file found for post'
      });
    }
    
    // Create a temp directory for processing
    const tempDir = path.join('/tmp', `audio-processing-${post.$id}`);
    try {
      await mkdirAsync(tempDir, { recursive: true });
      context.log(`Created temp directory: ${tempDir}`);
    } catch (err) {
      if (err.code !== 'EEXIST') {
        throw err;
      }
    }
    
    // Update post status to processing
    await databases.updateDocument(
      process.env.APPWRITE_DATABASE_ID,
      process.env.APPWRITE_COLLECTION_ID_POST,
      post.$id,
      {
        processing_status: 'processing',
        processing_progress: 10,
        processing_started_at: new Date().toISOString()
      }
    );
    
    context.log(`Updated post ${post.$id} status to processing`);
    
    // Download the audio file
    const audioFileName = `${post.$id}.mp3`;
    const audioFilePath = path.join(tempDir, audioFileName);
    
    context.log(`Downloading audio file ${post.audio_file_id} to ${audioFilePath}`);
    
    try {
      // Get the file data
      const fileData = await storage.getFileDownload(
        process.env.APPWRITE_BUCKET_ID,
        post.audio_file_id
      );
      
      // Save the file to disk
      await writeFileAsync(audioFilePath, Buffer.from(fileData));
      
      context.log(`Downloaded and saved audio file to ${audioFilePath}`);
      
      // Update progress
      await databases.updateDocument(
        process.env.APPWRITE_DATABASE_ID,
        process.env.APPWRITE_COLLECTION_ID_POST,
        post.$id,
        {
          processing_progress: 20
        }
      );
      
      // Upload mp3 as a processed file
      const mp3UploadResult = await storage.createFile(
        process.env.APPWRITE_BUCKET_ID,
        sdk.ID.unique(),
        sdk.InputFile.fromPath(audioFilePath),
        [`audio/${post.$id}`]
      );
      
      context.log(`Uploaded MP3 file, ID: ${mp3UploadResult.$id}`);
      
      // Generate HLS stream
      const hlsDir = path.join(tempDir, 'hls');
      try {
        await mkdirAsync(hlsDir, { recursive: true });
      } catch (err) {
        if (err.code !== 'EEXIST') {
          throw err;
        }
      }
      
      // Update progress
      await databases.updateDocument(
        process.env.APPWRITE_DATABASE_ID,
        process.env.APPWRITE_COLLECTION_ID_POST,
        post.$id,
        {
          processing_progress: 40
        }
      );
      
      // HLS output options
      const hlsOptions = {
        hls_time: 10,
        hls_playlist_type: 'vod',
        hls_segment_filename: path.join(hlsDir, 'segment_%03d.aac')
      };
      
      // Create HLS segments
      context.log('Creating HLS segments...');
      
      // HLS M3U8 path
      const m3u8Path = path.join(hlsDir, 'playlist.m3u8');
      
      // Process with FFMPEG
      await new Promise((resolve, reject) => {
        ffmpeg(audioFilePath)
          .output(m3u8Path)
          .audioCodec('aac')
          .audioBitrate('128k')
          .outputOptions([
            '-f hls',
            `-hls_time ${hlsOptions.hls_time}`,
            `-hls_playlist_type ${hlsOptions.hls_playlist_type}`,
            `-hls_segment_filename ${hlsOptions.hls_segment_filename}`
          ])
          .on('progress', progress => {
            context.log(`Processing: ${JSON.stringify(progress)}`);
          })
          .on('end', () => {
            context.log('HLS segmentation completed');
            resolve();
          })
          .on('error', err => {
            context.error('Error during HLS segmentation:', err);
            reject(err);
          })
          .run();
      });
      
      // Update progress
      await databases.updateDocument(
        process.env.APPWRITE_DATABASE_ID,
        process.env.APPWRITE_COLLECTION_ID_POST,
        post.$id,
        {
          processing_progress: 60
        }
      );
      
      // Read the generated playlist
      const playlistContent = await readFileAsync(m3u8Path, 'utf8');
      context.log(`Read playlist: ${m3u8Path}`);
      
      // Upload segments and get their IDs
      context.log('Uploading HLS segments...');
      const segmentFiles = fs.readdirSync(hlsDir).filter(file => file.endsWith('.aac'));
      
      const segmentIds = [];
      const segmentUrls = [];
      
      for (const segmentFile of segmentFiles) {
        const segmentPath = path.join(hlsDir, segmentFile);
        const segmentUploadResult = await storage.createFile(
          process.env.APPWRITE_BUCKET_ID,
          sdk.ID.unique(),
          sdk.InputFile.fromPath(segmentPath),
          [`audio/${post.$id}`]
        );
        
        const segmentDownloadUrl = storage.getFileDownload(
          process.env.APPWRITE_BUCKET_ID,
          segmentUploadResult.$id
        );
        
        segmentIds.push(segmentUploadResult.$id);
        segmentUrls.push(segmentDownloadUrl);
        
        context.log(`Uploaded segment ${segmentFile}, ID: ${segmentUploadResult.$id}`);
      }
      
      // Update progress
      await databases.updateDocument(
        process.env.APPWRITE_DATABASE_ID,
        process.env.APPWRITE_COLLECTION_ID_POST,
        post.$id,
        {
          processing_progress: 80
        }
      );
      
      // Create a modified playlist with the correct URLs
      let modifiedPlaylist = playlistContent;
      segmentFiles.forEach((segmentFile, index) => {
        modifiedPlaylist = modifiedPlaylist.replace(
          segmentFile,
          segmentUrls[index]
        );
      });
      
      const modifiedPlaylistPath = path.join(tempDir, 'modified_playlist.m3u8');
      await writeFileAsync(modifiedPlaylistPath, modifiedPlaylist);
      
      context.log('Created modified playlist with URLs');
      
      // Upload the modified playlist
      const playlistUploadResult = await storage.createFile(
        process.env.APPWRITE_BUCKET_ID,
        sdk.ID.unique(),
        sdk.InputFile.fromPath(modifiedPlaylistPath),
        [`audio/${post.$id}`]
      );
      
      context.log(`Uploaded modified playlist, ID: ${playlistUploadResult.$id}`);
      
      // Get the mp3 URL
      const mp3Url = storage.getFileDownload(
        process.env.APPWRITE_BUCKET_ID,
        mp3UploadResult.$id
      );
      
      // Get the playlist URL
      const playlistUrl = storage.getFileDownload(
        process.env.APPWRITE_BUCKET_ID,
        playlistUploadResult.$id
      );
      
      // Update post with processed audio
      await databases.updateDocument(
        process.env.APPWRITE_DATABASE_ID,
        process.env.APPWRITE_COLLECTION_ID_POST,
        post.$id,
        {
          mp3_file_id: mp3UploadResult.$id,
          mp3_url: mp3Url,
          hls_playlist_id: playlistUploadResult.$id,
          hls_playlist_url: playlistUrl,
          hls_segment_ids: segmentIds,
          processing_status: 'completed',
          processing_progress: 100,
          processing_completed_at: new Date().toISOString()
        }
      );
      
      context.log(`Updated post ${post.$id} with processed audio URLs`);
      
      // Clean up temp files
      try {
        // Delete all files in the temp directory
        for (const file of fs.readdirSync(tempDir)) {
          await unlinkAsync(path.join(tempDir, file));
        }
        
        // Delete the HLS directory
        for (const file of fs.readdirSync(hlsDir)) {
          await unlinkAsync(path.join(hlsDir, file));
        }
        
        fs.rmdirSync(hlsDir);
        fs.rmdirSync(tempDir);
        
        context.log('Cleaned up temporary files');
      } catch (cleanupError) {
        context.error('Error cleaning up temp files:', cleanupError);
        // Continue execution even if cleanup fails
      }
      
      // Return success response
      return context.res.json({
        success: true,
        message: 'Audio processing completed',
        postId: post.$id,
        mp3_file_id: mp3UploadResult.$id,
        hls_playlist_id: playlistUploadResult.$id
      });
    } catch (error) {
      context.error('Error processing audio:', error);
      
      try {
        // Update post with error status
        await databases.updateDocument(
          process.env.APPWRITE_DATABASE_ID,
          process.env.APPWRITE_COLLECTION_ID_POST,
          post.$id,
          {
            processing_status: 'failed',
            processing_error: error.message,
            processing_completed_at: new Date().toISOString()
          }
        );
        context.log(`Updated post ${post.$id} with error status`);
      } catch (updateError) {
        context.error('Failed to update post with error status:', updateError);
      }
      
      // Clean up temp directory if it exists
      try {
        if (fs.existsSync(tempDir)) {
          fs.rmSync(tempDir, { recursive: true, force: true });
          context.log('Cleaned up temporary directory after error');
        }
      } catch (cleanupError) {
        context.error('Error cleaning up temp directory:', cleanupError);
      }
      
      // Return error response
      return context.res.json({
        success: false,
        message: 'Error processing audio',
        error: error.message
      });
    }
  } catch (error) {
    context.error('Error processing audio:', error);
    
    try {
      if (payload && payload.postId) {
        await databases.updateDocument(
          process.env.APPWRITE_DATABASE_ID,
          process.env.APPWRITE_COLLECTION_ID_POST,
          payload.postId,
          {
            processing_status: 'failed',
            processing_error: error.message,
            processing_completed_at: new Date().toISOString()
          }
        );
        context.log(`Updated post ${payload.postId} with error status`);
      }
    } catch (updateError) {
      context.error('Failed to update post with error status:', updateError);
    }
    
    // Ensure context.res exists before trying to use it
    return context.res.json({
      success: false,
      message: 'Error processing audio',
      error: error.message
    });
  }
};

// Alternate export format for Appwrite Functions compatibility
// This ensures that if Appwrite calls our function with a single context object,
// it will still work correctly
exports.handler = async (context) => {
  if (!context) {
    console.log('[APPWRITE_FUNCTION] Warning: context is undefined in handler');
    context = { req: {}, res: { json: (data) => data } };
  }
  
  console.log('[APPWRITE_FUNCTION] Function called via handler method');
  // Call our main implementation with the context object
  return await module.exports(context);
}; 