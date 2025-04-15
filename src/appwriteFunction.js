// Appwrite Function for audio processing
// Optimized for direct calls from Sacral Track application

import { Client, Databases, Storage, ID } from 'node-appwrite';
import * as fs from 'fs';
import * as path from 'path';
import { promisify } from 'util';
import ffmpeg from 'fluent-ffmpeg';

// Main entry point for Appwrite Function - Single context parameter format
export default async function(context) {
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
  log('- Method:', req.method || 'unknown');
  log('- URL:', req.url || 'unknown');
  if (req.headers) {
    log('- Content-Type:', req.headers['content-type'] || 'none');
    log('- Trigger:', req.headers['x-appwrite-trigger'] || 'none');
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
      try {
        if (typeof req.body === 'string') {
          log('Body is string, length:', req.body.length);
          if (req.body.trim().length > 0) {
            try {
              payload = JSON.parse(req.body);
              log('Parsed JSON payload successfully');
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
    
    // Try to extract postId from various sources
    postId = payload.postId;
    
    // Check URL parameters
    if (!postId && req.url) {
      try {
        const url = new URL(req.url);
        const params = new URLSearchParams(url.search);
        if (params.has('postId')) {
          postId = params.get('postId');
          log(`Found postId in URL params: ${postId}`);
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
        // For document events, try to extract document ID
        if (req.bodyJson && req.bodyJson.$id) {
          postId = req.bodyJson.$id;
          log(`Extracted postId from event: ${postId}`);
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
      
      // Return success response
      return res.json({
        success: true,
        message: 'Audio processing completed',
        postId: post.$id,
        mp3_file_id: mp3UploadResult.$id,
        hls_playlist_id: playlistUploadResult.$id
      });
    } catch (error) {
      logError('Error processing audio:', error);
      
      try {
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
    
    try {
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