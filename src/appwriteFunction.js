// Appwrite Function for audio processing
// Optimized for direct calls from Sacral Track application

import { Client, Databases, Storage, ID } from 'node-appwrite';
import * as fs from 'fs';
import * as path from 'path';
import { promisify } from 'util';
import ffmpeg from 'fluent-ffmpeg';

// Main entry point for Appwrite Function - Single context parameter format
export default async function(context) {
  // Start tracking execution time
  const startTime = Date.now();
  
  // Initialize a safe execution status record
  let executionRecord = {
    timestamp: new Date().toISOString(),
    status: 'started',
    trigger_type: 'unknown',
    error: null,
    parsing_error: null,
    request_info: {},
    execution_time_ms: 0
  };
  
  // Log the full context type and structure for debugging
  console.log('[APPWRITE_FUNCTION] Context type:', typeof context);
  console.log('[APPWRITE_FUNCTION] Context keys:', context ? Object.keys(context).join(', ') : 'undefined');
  
  // Safely extract functions from context with fallbacks
  const req = context.req || {};
  const res = {
    json: (data) => {
      if (context.res && typeof context.res.json === 'function') {
        return context.res.json(data);
      }
      // Fallback if res.json doesn't exist
      console.log('[APPWRITE_FUNCTION] res.json fallback used:', JSON.stringify(data));
      return data;
    }
  };
  
  // Create safe logging functions
  const log = (context.log && typeof context.log === 'function') 
    ? context.log 
    : (...args) => console.log('[APPWRITE_FUNCTION]', ...args);
    
  const logError = (context.error && typeof context.error === 'function')
    ? context.error
    : (...args) => console.error('[APPWRITE_FUNCTION_ERROR]', ...args);
  
  log('Audio processing function started');
  log('Request details:');
  
  // Store basic request info
  executionRecord.request_info = {
    method: req.method || 'unknown',
    url: req.url || 'unknown',
    content_type: req.headers ? (req.headers['content-type'] || 'none') : 'none',
    trigger: req.headers ? (req.headers['x-appwrite-trigger'] || 'none') : 'none'
  };
  
  executionRecord.trigger_type = executionRecord.request_info.trigger;
  
  log('- Method:', executionRecord.request_info.method);
  log('- URL:', executionRecord.request_info.url);
  if (req.headers) {
    log('- Content-Type:', executionRecord.request_info.content_type);
    log('- Trigger:', executionRecord.request_info.trigger);
  }
  
  // Promisify fs functions
  const mkdirAsync = promisify(fs.mkdir);
  const writeFileAsync = promisify(fs.writeFile);
  const readFileAsync = promisify(fs.readFile);
  const unlinkAsync = promisify(fs.unlink);
  
  // Initialize Appwrite client
  const client = new Client();
  
  // Get environment variables
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
  
  const databases = new Databases(client);
  const storage = new Storage(client);
  
  // Initialize payload and postId as empty at the function level to avoid undefined errors
  let payload = {};
  let postId = null;
  
  try {
    // Parse request body and extract payload
    
    // DEBUG: Log the full raw request body for debugging
    if (req.body !== undefined) {
      log('DEBUG - Request body exists');
      
      // Detailed logging for debugging
      log('Request body type:', typeof req.body);
      if (typeof req.body === 'string') {
        if (req.body.length < 1000) {
          log('Raw request body:', req.body);
        } else {
          log('Raw request body (truncated):', req.body.substring(0, 500) + '...');
        }
      } else {
        try {
          log('Request body (JSON):', JSON.stringify(req.body).substring(0, 500) + '...');
        } catch (e) {
          log('Request body: [Cannot stringify body]');
        }
      }
      
      // Safe parse body
      try {
        // Explicitly log the raw body for debugging
        if (typeof req.body === 'string') {
          log('Raw body as string:', req.body);
          
          // Check for non-printable characters
          const hasNonPrintable = /[\x00-\x1F\x7F-\x9F]/.test(req.body);
          if (hasNonPrintable) {
            log('WARNING: Non-printable characters detected in the body');
            // Show hex representation for debugging
            const hexBody = Array.from(req.body).map(c => c.charCodeAt(0).toString(16).padStart(2, '0')).join(' ');
            log('Body as hex (first 100 bytes):', hexBody.substring(0, 300));
          }
        } else {
          log('Raw body type:', typeof req.body);
        }
        
        if (typeof req.body === 'string') {
          log('Body is string, length:', req.body.length);
          if (req.body.trim().length > 0) {
            try {
              // Try to safely parse the JSON with more aggressive cleaning
              // Strip BOM, whitespace, and other common problematic characters
              let cleanedBody = req.body
                .replace(/^\uFEFF/, '')                  // BOM
                .replace(/^\s+|\s+$/g, '')               // Leading/trailing whitespace
                .replace(/\u200B/g, '')                  // Zero-width space
                .replace(/[\x00-\x1F\x7F-\x9F]/g, '');   // Control characters
              
              // Find the first '{' and last '}' for JSON objects, or first '[' and last ']' for arrays
              const firstBrace = cleanedBody.indexOf('{');
              const lastBrace = cleanedBody.lastIndexOf('}');
              const firstBracket = cleanedBody.indexOf('[');
              const lastBracket = cleanedBody.lastIndexOf(']');
              
              // Determine if this looks like a JSON object or array
              let jsonContent = cleanedBody;
              
              if (firstBrace >= 0 && lastBrace > firstBrace) {
                // Extract just the JSON object
                jsonContent = cleanedBody.substring(firstBrace, lastBrace + 1);
                log('Extracted JSON object from content');
              } else if (firstBracket >= 0 && lastBracket > firstBracket) {
                // Extract just the JSON array
                jsonContent = cleanedBody.substring(firstBracket, lastBracket + 1);
                log('Extracted JSON array from content');
              }
              
              // Check if the body is already a valid JSON string
              if ((jsonContent.startsWith('{') && jsonContent.endsWith('}')) || 
                  (jsonContent.startsWith('[') && jsonContent.endsWith(']'))) {
                try {
                  log('Attempting to parse cleaned JSON:', jsonContent.substring(0, 100) + (jsonContent.length > 100 ? '...' : ''));
                  payload = JSON.parse(jsonContent);
                  log('Parsed JSON payload successfully');
                } catch (parseError) {
                  logError('JSON parse error after cleaning:', parseError.message);
                  // Try to extract postId directly if JSON parsing fails
                  const postIdMatch = jsonContent.match(/"postId"\s*:\s*"([^"]+)"/);
                  if (postIdMatch && postIdMatch[1]) {
                    payload = { postId: postIdMatch[1] };
                    log('Extracted postId from invalid JSON:', postIdMatch[1]);
                  } else {
                    payload = { postId: jsonContent };
                    log('Using cleaned content as postId fallback');
                  }
                }
              } else {
                // If not a JSON object, use as raw postId
                payload = { postId: cleanedBody };
                log('Using cleaned body as postId (not a JSON object)');
              }
            } catch (parseError) {
              logError('JSON parse error:', parseError.message);
              payload = { postId: req.body.trim() };
              log('Using raw body as postId fallback');
            }
          }
        } else if (typeof req.body === 'object') {
          payload = req.body;
          log('Body is already an object');
        }
      } catch (bodyError) {
        logError('Error processing body:', bodyError);
      }
    }
    
    // Safely try to access bodyJson if available
    try {
      if (req.bodyJson) {
        if (typeof req.bodyJson === 'object') {
          log('bodyJson is available and is an object, using it as payload');
          payload = req.bodyJson;
        } else if (typeof req.bodyJson === 'string') {
          // Try to parse bodyJson if it's a string
          try {
            const parsedJson = JSON.parse(req.bodyJson);
            log('Parsed bodyJson string to object');
            payload = parsedJson;
          } catch (parseError) {
            logError('Failed to parse bodyJson string:', parseError.message);
          }
        } else {
          log('bodyJson is available but type is unexpected:', typeof req.bodyJson);
        }
      }
    } catch (jsonError) {
      logError('Error accessing bodyJson:', jsonError);
    }
    
    // Try to extract postId from various sources
    postId = payload.postId;
    
    // Check URL parameters
    if (!postId && req.url) {
      try {
        log('Checking URL parameters for postId');
        
        // Enhanced URL parsing with fallbacks
        let url;
        let params;
        
        try {
          // Regular URL parsing
          url = new URL(req.url);
          params = new URLSearchParams(url.search);
          log('Parsed URL:', url.toString());
          log('URL has search params:', url.search ? 'yes' : 'no');
        } catch (urlParseError) {
          // Fallback for relative URLs or non-standard URLs
          logError('Standard URL parsing failed, trying fallback:', urlParseError.message);
          
          const queryPart = req.url.split('?')[1];
          if (queryPart) {
            params = new URLSearchParams(queryPart);
            log('Parsed URL params using fallback method');
          } else {
            log('No query parameters found in URL');
            params = new URLSearchParams();
          }
        }
        
        // Check for postId in URL params
        if (params.has('postId')) {
          postId = params.get('postId');
          log(`Found postId in URL params: ${postId}`);
        } else {
          // Additional check for ID in URL path
          const pathParts = req.url.split('/');
          const lastPart = pathParts[pathParts.length - 1];
          
          // Check if last part could be an ID (no slashes, reasonable length)
          if (lastPart && lastPart.length > 5 && lastPart.length < 30 && !lastPart.includes('?')) {
            log(`Potential ID found in URL path: ${lastPart}`);
            if (!postId) {
              postId = lastPart;
              log(`Using URL path component as postId: ${postId}`);
            }
          }
        }
      } catch (urlError) {
        logError('URL parsing error:', urlError);
      }
    }
    
    // Check for event payload
    if (!postId && 
        req.headers && 
        req.headers['x-appwrite-trigger'] === 'event' && 
        req.headers['x-appwrite-event'] && 
        req.headers['x-appwrite-event'].includes('documents')) {
      try {
        log('Detected Appwrite document event');
        
        // Safe access to req.bodyJson for events
        let eventData = null;
        
        // Try multiple sources for event data
        if (req.bodyJson) {
          log('Using req.bodyJson for event data');
          eventData = req.bodyJson;
        } else if (payload && typeof payload === 'object') {
          log('Using payload for event data');
          eventData = payload;
        }
        
        // Try to find document ID in multiple places
        if (eventData) {
          if (eventData.$id) {
            postId = eventData.$id;
            log(`Extracted postId from event $id: ${postId}`);
          } else if (eventData.record && eventData.record.$id) {
            postId = eventData.record.$id;
            log(`Extracted postId from event record $id: ${postId}`);
          } else if (eventData.document && eventData.document.$id) {
            postId = eventData.document.$id;
            log(`Extracted postId from event document $id: ${postId}`);
          }
        }
        
        // Log if we couldn't find an ID
        if (!postId) {
          log('Could not extract postId from event data');
          if (eventData) {
            log('Event data keys:', Object.keys(eventData || {}).join(', '));
          }
        }
      } catch (eventError) {
        logError('Event parsing error:', eventError);
      }
    }
    
    // Test mode detection
    const isTestMode = req.headers && req.headers['x-test-mode'] === 'true';
    if (isTestMode) {
      log('TEST MODE DETECTED - Using test postId');
      postId = '656b66339fa5f91c2b73';
    }
    
    // Final check before proceeding
    if (!postId) {
      log('No postId found in request');
      return res.json({
        success: false,
        message: 'No postId found in request'
      });
    }
    
    log(`Processing audio for post: ${postId}`);
    
    // Get post document
    const post = await databases.getDocument(
      APPWRITE_DATABASE_ID,
      APPWRITE_COLLECTION_ID_POST,
      postId
    );
    
    if (!post) {
      log(`Post not found: ${postId}`);
      return res.json({
        success: false,
        message: 'Post not found'
      });
    }
    
    log(`Retrieved post: ${post.$id}`);
    
    if (!post.audio_file_id) {
      return res.json({
        success: false,
        message: 'No audio file found for post'
      });
    }
    
    // Create a temp directory for processing
    const tempDir = path.join('/tmp', `audio-processing-${post.$id}`);
    try {
      await mkdirAsync(tempDir, { recursive: true });
      log(`Created temp directory: ${tempDir}`);
    } catch (err) {
      if (err.code !== 'EEXIST') {
        throw err;
      }
    }
    
    // Update post status to processing
    await databases.updateDocument(
      APPWRITE_DATABASE_ID,
      APPWRITE_COLLECTION_ID_POST,
      post.$id,
      {
        processing_status: 'processing',
        processing_progress: 10,
        processing_started_at: new Date().toISOString()
      }
    );
    
    log(`Updated post ${post.$id} status to processing`);
    
    // Download the audio file
    const audioFileName = `${post.$id}.mp3`;
    const audioFilePath = path.join(tempDir, audioFileName);
    
    log(`Downloading audio file ${post.audio_file_id} to ${audioFilePath}`);
    
    try {
      // Get the file data
      const fileData = await storage.getFileDownload(
        APPWRITE_BUCKET_ID,
        post.audio_file_id
      );
      
      // Save the file to disk
      await writeFileAsync(audioFilePath, Buffer.from(fileData));
      
      log(`Downloaded and saved audio file to ${audioFilePath}`);
      
      // Update progress
      await databases.updateDocument(
        APPWRITE_DATABASE_ID,
        APPWRITE_COLLECTION_ID_POST,
        post.$id,
        {
          processing_progress: 20
        }
      );
      
      // Upload mp3 as a processed file
      const mp3UploadResult = await storage.createFile(
        APPWRITE_BUCKET_ID,
        ID.unique(),
        {
          path: audioFilePath,
          type: 'audio/mpeg'
        },
        [`audio/${post.$id}`]
      );
      
      log(`Uploaded MP3 file, ID: ${mp3UploadResult.$id}`);
      
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
        APPWRITE_DATABASE_ID,
        APPWRITE_COLLECTION_ID_POST,
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
      log('Creating HLS segments...');
      
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
            log(`Processing: ${JSON.stringify(progress)}`);
          })
          .on('end', () => {
            log('HLS segmentation completed');
            resolve();
          })
          .on('error', err => {
            logError('Error during HLS segmentation:', err);
            reject(err);
          })
          .run();
      });
      
      // Update progress
      await databases.updateDocument(
        APPWRITE_DATABASE_ID,
        APPWRITE_COLLECTION_ID_POST,
        post.$id,
        {
          processing_progress: 60
        }
      );
      
      // Read the generated playlist
      const playlistContent = await readFileAsync(m3u8Path, 'utf8');
      log(`Read playlist: ${m3u8Path}`);
      
      // Upload segments and get their IDs
      log('Uploading HLS segments...');
      const segmentFiles = fs.readdirSync(hlsDir).filter(file => file.endsWith('.aac'));
      
      const segmentIds = [];
      const segmentUrls = [];
      
      for (const segmentFile of segmentFiles) {
        const segmentPath = path.join(hlsDir, segmentFile);
        const segmentUploadResult = await storage.createFile(
          APPWRITE_BUCKET_ID,
          ID.unique(),
          {
            path: segmentPath,
            type: 'audio/aac' 
          },
          [`audio/${post.$id}`]
        );
        
        const segmentDownloadUrl = storage.getFileDownload(
          APPWRITE_BUCKET_ID,
          segmentUploadResult.$id
        );
        
        segmentIds.push(segmentUploadResult.$id);
        segmentUrls.push(segmentDownloadUrl);
        
        log(`Uploaded segment ${segmentFile}, ID: ${segmentUploadResult.$id}`);
      }
      
      // Update progress
      await databases.updateDocument(
        APPWRITE_DATABASE_ID,
        APPWRITE_COLLECTION_ID_POST,
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
      
      log('Created modified playlist with URLs');
      
      // Upload the modified playlist
      const playlistUploadResult = await storage.createFile(
        APPWRITE_BUCKET_ID,
        ID.unique(),
        {
          path: modifiedPlaylistPath,
          type: 'application/vnd.apple.mpegurl'
        },
        [`audio/${post.$id}`]
      );
      
      log(`Uploaded modified playlist, ID: ${playlistUploadResult.$id}`);
      
      // Get the mp3 URL
      const mp3Url = storage.getFileDownload(
        APPWRITE_BUCKET_ID,
        mp3UploadResult.$id
      );
      
      // Get the playlist URL
      const playlistUrl = storage.getFileDownload(
        APPWRITE_BUCKET_ID,
        playlistUploadResult.$id
      );
      
      // Update post with processed audio
      await databases.updateDocument(
        APPWRITE_DATABASE_ID,
        APPWRITE_COLLECTION_ID_POST,
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
      
      log(`Updated post ${post.$id} with processed audio URLs`);
      
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
        
        log('Cleaned up temporary files');
      } catch (cleanupError) {
        logError('Error cleaning up temp files:', cleanupError);
        // Continue execution even if cleanup fails
      }
      
      // Update execution record
      executionRecord.status = 'completed';
      executionRecord.execution_time_ms = Date.now() - startTime;
      
      // Log execution attempt to database regardless of success/failure
      try {
        // Create a processing event record if we have client connection
        if (databases && APPWRITE_DATABASE_ID) {
          const recordId = ID.unique();
          const eventData = {
            function_id: process.env.APPWRITE_FUNCTION_ID || 'unknown',
            execution_id: recordId,
            timestamp: executionRecord.timestamp,
            status: executionRecord.status,
            error: executionRecord.error,
            trigger_type: executionRecord.trigger_type,
            request_info: JSON.stringify(executionRecord.request_info),
            post_id: post ? post.$id : (postId || 'unknown'),
            execution_time_ms: executionRecord.execution_time_ms
          };
          
          try {
            await databases.createDocument(
              APPWRITE_DATABASE_ID,
              'function_executions', // Create this collection in your Appwrite dashboard
              recordId,
              eventData
            );
            log(`Logged execution event: ${recordId}`);
          } catch (logError) {
            console.error('Failed to log execution event:', logError);
          }
        }
      } catch (logError) {
        console.error('Failed to create execution log:', logError);
      }
      
      // Ensure postId is defined before using it
      const safePostId = (post ? post.$id : null) || postId || (payload && payload.postId) || null;
      
      if (safePostId) {
        await databases.updateDocument(
          APPWRITE_DATABASE_ID,
          APPWRITE_COLLECTION_ID_POST,
          safePostId,
          {
            processing_status: 'completed',
            processing_progress: 100,
            processing_completed_at: new Date().toISOString()
          }
        );
        log(`Updated post ${safePostId} with completed status`);
      } else {
        log('Cannot update post with completed status: postId is not defined');
      }
      
      // Return success response
      return res.json({
        success: true,
        message: 'Audio processing completed',
        postId: safePostId,
        mp3_file_id: mp3UploadResult.$id,
        hls_playlist_id: playlistUploadResult.$id
      });
    } catch (error) {
      logError('Error processing audio:', error);
      
      // Update execution record
      executionRecord.status = 'error';
      executionRecord.error = error.message;
      executionRecord.execution_time_ms = Date.now() - startTime;
      
      try {
        // Log execution attempt to database regardless of success/failure
        try {
          // Create a processing event record if we have client connection
          if (databases && APPWRITE_DATABASE_ID) {
            const recordId = ID.unique();
            const eventData = {
              function_id: process.env.APPWRITE_FUNCTION_ID || 'unknown',
              execution_id: recordId,
              timestamp: executionRecord.timestamp,
              status: executionRecord.status,
              error: executionRecord.error,
              trigger_type: executionRecord.trigger_type,
              request_info: JSON.stringify(executionRecord.request_info),
              post_id: post ? post.$id : (postId || 'unknown'),
              execution_time_ms: executionRecord.execution_time_ms
            };
            
            try {
              await databases.createDocument(
                APPWRITE_DATABASE_ID,
                'function_executions', // Create this collection in your Appwrite dashboard
                recordId,
                eventData
              );
              log(`Logged execution event: ${recordId}`);
            } catch (logError) {
              console.error('Failed to log execution event:', logError);
            }
          }
        } catch (logError) {
          console.error('Failed to create execution log:', logError);
        }
        
        // Ensure postId is defined before using it
        const safePostId = (post ? post.$id : null) || postId || (payload && payload.postId) || null;
        
        if (safePostId) {
          await databases.updateDocument(
            APPWRITE_DATABASE_ID,
            APPWRITE_COLLECTION_ID_POST,
            safePostId,
            {
              processing_status: 'failed',
              processing_error: error.message,
              processing_completed_at: new Date().toISOString()
            }
          );
          log(`Updated post ${safePostId} with error status`);
        } else {
          log('Cannot update post with error status: postId is not defined');
        }
      } catch (updateError) {
        logError('Failed to update post with error status:', updateError);
      }
      
      // Clean up temp directory if it exists
      try {
        if (fs.existsSync(tempDir)) {
          fs.rmSync(tempDir, { recursive: true, force: true });
          log('Cleaned up temporary directory after error');
        }
      } catch (cleanupError) {
        logError('Error cleaning up temp directory:', cleanupError);
      }
      
      // Return error response
      return res.json({
        success: false,
        message: 'Error processing audio',
        error: error.message
      });
    }
  } catch (error) {
    logError('Error processing audio:', error);
    
    // Update execution record
    executionRecord.status = 'error';
    executionRecord.error = error.message;
    executionRecord.execution_time_ms = Date.now() - startTime;
    
    try {
      // Log execution attempt to database regardless of success/failure
      try {
        // Create a processing event record if we have client connection
        if (databases && APPWRITE_DATABASE_ID) {
          const recordId = ID.unique();
          const eventData = {
            function_id: process.env.APPWRITE_FUNCTION_ID || 'unknown',
            execution_id: recordId,
            timestamp: executionRecord.timestamp,
            status: executionRecord.status,
            error: executionRecord.error,
            trigger_type: executionRecord.trigger_type,
            request_info: JSON.stringify(executionRecord.request_info),
            post_id: postId || 'unknown',
            execution_time_ms: executionRecord.execution_time_ms
          };
          
          try {
            await databases.createDocument(
              APPWRITE_DATABASE_ID,
              'function_executions', // Create this collection in your Appwrite dashboard
              recordId,
              eventData
            );
            log(`Logged execution event: ${recordId}`);
          } catch (logError) {
            console.error('Failed to log execution event:', logError);
          }
        }
      } catch (logError) {
        console.error('Failed to create execution log:', logError);
      }
      
      // Ensure postId is defined before using it
      const safePostId = postId || (payload && payload.postId) || null;
      
      if (safePostId) {
        await databases.updateDocument(
          APPWRITE_DATABASE_ID,
          APPWRITE_COLLECTION_ID_POST,
          safePostId,
          {
            processing_status: 'failed',
            processing_error: error.message,
            processing_completed_at: new Date().toISOString()
          }
        );
        log(`Updated post ${safePostId} with error status`);
      } else {
        log('Cannot update post with error status: postId is not defined');
      }
    } catch (updateError) {
      logError('Failed to update post with error status:', updateError);
    }
    
    // Return error response
    return res.json({
      success: false,
      message: 'Error processing audio',
      error: error.message
    });
  }
}

// Alternate export format for Appwrite Functions compatibility
// This ensures that if Appwrite calls our function with a single context object,
// it will still work correctly
export const handler = async (context) => {
  console.log('[APPWRITE_FUNCTION] Handler called with context:', typeof context);
  
  if (!context) {
    console.log('[APPWRITE_FUNCTION] Warning: context is undefined in handler');
    context = { req: {}, res: { json: (data) => data } };
  }
  
  // Ensure req and res exist
  if (!context.req) {
    console.log('[APPWRITE_FUNCTION] Warning: context.req is undefined, creating empty object');
    context.req = {};
  }
  
  if (!context.res || typeof context.res.json !== 'function') {
    console.log('[APPWRITE_FUNCTION] Warning: context.res or res.json is undefined, creating fallback');
    context.res = {
      json: (data) => {
        console.log('[APPWRITE_FUNCTION] Using fallback res.json:', JSON.stringify(data));
        return data;
      }
    };
  }
  
  console.log('[APPWRITE_FUNCTION] Function called via handler method');
  // Call our main implementation with the context object
  return await exports.default(context);
}; 