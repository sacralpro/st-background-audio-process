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
  
  // Ensure res has json method
  if (!context.res.json) {
    context.res.json = function(data) {
      return data;
    };
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
    
    // Parse request body if it's a string
    if (context.req && context.req.body) {
      if (typeof context.req.body === 'string') {
        try {
          // Ensure the string is clean before parsing
          const trimmedBody = context.req.body.trim();
          context.log('Request body string length:', trimmedBody.length);
          
          // For debugging, log the first 100 characters
          if (trimmedBody.length > 0) {
            context.log('First 100 chars of body:', trimmedBody.substring(0, 100));
          }
          
          payload = JSON.parse(trimmedBody);
        } catch (parseError) {
          context.error('Error parsing request body:', parseError.message);
          return context.res.json({
            success: false,
            message: `Invalid JSON in request body: ${parseError.message}`
          });
        }
      } else if (typeof context.req.body === 'object') {
        payload = context.req.body;
      }
    }
    
    // Log the payload
    if (Object.keys(payload).length > 0) {
      context.log('Payload:', JSON.stringify(payload).substring(0, 200) + '...');
    } else {
      context.log('Empty payload received');
    }
    
    // Extract postId from payload
    const { postId } = payload;
    
    if (!postId) {
      context.log('No postId found in payload');
      return context.res.json({
        success: false,
        message: 'No postId found in request'
      });
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