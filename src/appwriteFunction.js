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
  // Initialize Appwrite SDK
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
    console.log('Starting audio processing function...');
    
    const payload = req.body || {};
    let postId = payload.postId || null;
    
    if (!postId && payload.event && payload.payload && payload.payload.$id) {
      if (payload.event.includes('documents') && 
          (payload.event.includes('create') || payload.event.includes('update'))) {
        postId = payload.payload.$id;
        console.log(`Event trigger for post: ${postId} via ${payload.event}`);
      } else {
        if (res) {
          return res.json({
            success: false,
            message: 'Unsupported event type'
          });
        }
        return res ? res.send() : undefined;
      }
    } 
    
    if (!postId) {
      if (res) {
        return res.json({
          success: false,
          message: 'Missing postId in payload'
        });
      }
      return res ? res.send() : undefined;
    }
    
    console.log(`Fetching post document: ${postId}`);
    const post = await databases.getDocument(
      APPWRITE_DATABASE_ID,
      APPWRITE_COLLECTION_ID_POST,
      postId
    );
    
    if (!post.audio_url) {
      console.log(`Post ${postId} has no audio file to process`);
      if (res) {
        return res.json({
          success: false,
          message: 'Post has no audio file to process'
        });
      }
      return res ? res.send() : undefined;
    }
    
    if (post.mp3_url) {
      console.log(`Post ${postId} is already processed`);
      if (res) {
        return res.json({
          success: true,
          message: 'Post already processed',
          mp3_url: post.mp3_url,
          m3u8_url: post.m3u8_url
        });
      }
      return res ? res.send() : undefined;
    }
    
    console.log(`Updating post ${postId} status to processing`);
    await databases.updateDocument(
      APPWRITE_DATABASE_ID,
      APPWRITE_COLLECTION_ID_POST,
      postId,
      {
        processing_status: 'processing',
        processing_started_at: new Date().toISOString()
      }
    );
    
    // Create temp directory for processing
    const tempDir = '/tmp/audio-processing';
    await mkdirAsync(tempDir, { recursive: true });
    
    // Extract file ID from audio_url
    const audioUrl = post.audio_url;
    console.log(`Original audio URL: ${audioUrl}`);
    const wavFileId = audioUrl.split('/files/')[1].split('/')[0];
    console.log(`Extracted file ID: ${wavFileId}`);
    
    // Create base name for files
    const baseName = `track_${post.$id}`;
    const wavPath = path.join(tempDir, `${baseName}.wav`);
    const mp3Path = path.join(tempDir, `${baseName}.mp3`);
    
    // Step 1: Download WAV file
    console.log(`Downloading audio file: ${wavFileId}`);
    const fileBlob = await storage.getFileDownload(APPWRITE_BUCKET_ID, wavFileId);
    const arrayBuffer = await fileBlob.arrayBuffer();
    const buffer = Buffer.from(arrayBuffer);
    await writeFileAsync(wavPath, buffer);
    console.log(`Audio file downloaded to: ${wavPath}`);
    
    // Update progress
    await databases.updateDocument(
      APPWRITE_DATABASE_ID,
      APPWRITE_COLLECTION_ID_POST,
      postId,
      {
        processing_progress: 'Downloading audio file completed'
      }
    );
    
    // Step 2: Convert WAV to MP3
    console.log('Starting WAV to MP3 conversion');
    await new Promise((resolve, reject) => {
      ffmpeg(wavPath)
        .audioCodec('libmp3lame')
        .audioBitrate('192k')
        .format('mp3')
        .on('progress', (progress) => {
          console.log(`MP3 Conversion: ${JSON.stringify(progress)}`);
        })
        .on('error', reject)
        .on('end', resolve)
        .save(mp3Path);
    });
    console.log(`MP3 conversion completed: ${mp3Path}`);
    
    // Update progress
    await databases.updateDocument(
      APPWRITE_DATABASE_ID,
      APPWRITE_COLLECTION_ID_POST,
      postId,
      {
        processing_progress: 'MP3 conversion completed'
      }
    );
    
    // Step 3: Upload MP3 to Appwrite
    console.log('Uploading MP3 to storage');
    const mp3Buffer = await readFileAsync(mp3Path);
    const mp3File = await storage.createFile(
      APPWRITE_BUCKET_ID,
      sdk.ID.unique(),
      mp3Buffer,
      `${baseName}.mp3`
    );
    console.log(`MP3 file uploaded with ID: ${mp3File.$id}`);
    
    // Update progress
    await databases.updateDocument(
      APPWRITE_DATABASE_ID,
      APPWRITE_COLLECTION_ID_POST,
      postId,
      {
        processing_progress: 'MP3 upload completed',
        mp3_url: `${APPWRITE_ENDPOINT}/storage/buckets/${APPWRITE_BUCKET_ID}/files/${mp3File.$id}/view?project=${APPWRITE_FUNCTION_PROJECT_ID}`
      }
    );
    
    // Step 4: Create HLS segments
    console.log('Creating HLS segments');
    const segmentsDir = path.join(tempDir, 'segments');
    await mkdirAsync(segmentsDir, { recursive: true });
    
    const segmentsPattern = path.join(segmentsDir, `${baseName}_%03d.ts`);
    const playlistPath = path.join(tempDir, `${baseName}.m3u8`);
    
    await new Promise((resolve, reject) => {
      ffmpeg(mp3Path)
        .audioCodec('aac')
        .audioBitrate('128k')
        .format('hls')
        .outputOptions([
          '-hls_time 10',
          '-hls_list_size 0',
          '-hls_segment_type mpegts',
          '-hls_segment_filename', segmentsPattern
        ])
        .on('progress', (progress) => {
          console.log(`HLS Segmentation: ${JSON.stringify(progress)}`);
        })
        .on('error', reject)
        .on('end', resolve)
        .save(playlistPath);
    });
    console.log('HLS segmentation completed');
    
    // Update progress
    await databases.updateDocument(
      APPWRITE_DATABASE_ID,
      APPWRITE_COLLECTION_ID_POST,
      postId,
      {
        processing_progress: 'HLS segmentation completed'
      }
    );
    
    // Step 5: Upload segments and collect IDs
    console.log('Uploading HLS segments');
    const segmentFiles = fs.readdirSync(segmentsDir);
    const segmentFileIds = {};
    const streamingUrls = [];
    
    for (const segmentFile of segmentFiles) {
      if (segmentFile.endsWith('.ts')) {
        const segmentPath = path.join(segmentsDir, segmentFile);
        const segmentBuffer = await readFileAsync(segmentPath);
        
        const segment = await storage.createFile(
          APPWRITE_BUCKET_ID,
          sdk.ID.unique(),
          segmentBuffer,
          segmentFile
        );
        
        segmentFileIds[segmentFile] = segment.$id;
        
        const streamingUrl = `${APPWRITE_ENDPOINT}/storage/buckets/${APPWRITE_BUCKET_ID}/files/${segment.$id}/view?project=${APPWRITE_FUNCTION_PROJECT_ID}`;
        streamingUrls.push(streamingUrl);
      }
    }
    console.log(`Uploaded ${Object.keys(segmentFileIds).length} HLS segments`);
    
    // Update progress
    await databases.updateDocument(
      APPWRITE_DATABASE_ID,
      APPWRITE_COLLECTION_ID_POST,
      postId,
      {
        processing_progress: 'HLS segments uploaded'
      }
    );
    
    // Step 6: Create and upload modified playlist
    console.log('Creating and uploading modified playlist');
    const playlistContent = await readFileAsync(playlistPath, 'utf8');
    let newPlaylistContent = playlistContent;
    
    Object.keys(segmentFileIds).forEach((segmentName) => {
      const fileId = segmentFileIds[segmentName];
      newPlaylistContent = newPlaylistContent.replace(
        new RegExp(segmentName, 'g'),
        `${APPWRITE_ENDPOINT}/storage/buckets/${APPWRITE_BUCKET_ID}/files/${fileId}/view?project=${APPWRITE_FUNCTION_PROJECT_ID}`
      );
    });
    
    const newPlaylistPath = path.join(tempDir, 'appwrite_playlist.m3u8');
    await writeFileAsync(newPlaylistPath, newPlaylistContent, 'utf8');
    
    const playlistBuffer = await readFileAsync(newPlaylistPath);
    const playlist = await storage.createFile(
      APPWRITE_BUCKET_ID,
      sdk.ID.unique(),
      playlistBuffer,
      `${baseName}.m3u8`
    );
    console.log(`Playlist uploaded with ID: ${playlist.$id}`);
    
    // Step 7: Update post with all processed audio information
    console.log(`Updating post ${postId} with all processed information`);
    await databases.updateDocument(
      APPWRITE_DATABASE_ID,
      APPWRITE_COLLECTION_ID_POST,
      post.$id,
      {
        mp3_url: `${APPWRITE_ENDPOINT}/storage/buckets/${APPWRITE_BUCKET_ID}/files/${mp3File.$id}/view?project=${APPWRITE_FUNCTION_PROJECT_ID}`,
        m3u8_url: `${APPWRITE_ENDPOINT}/storage/buckets/${APPWRITE_BUCKET_ID}/files/${playlist.$id}/view?project=${APPWRITE_FUNCTION_PROJECT_ID}`,
        segments: JSON.stringify(Object.values(segmentFileIds)),
        streaming_urls: streamingUrls,
        processing_status: 'completed',
        processing_completed_at: new Date().toISOString(),
        processing_progress: 'All processing completed'
      }
    );
    
    // Clean up
    console.log('Cleaning up temporary files');
    await unlinkAsync(wavPath);
    await unlinkAsync(mp3Path);
    await unlinkAsync(newPlaylistPath);
    await unlinkAsync(playlistPath);
    for (const file of segmentFiles) {
      await unlinkAsync(path.join(segmentsDir, file));
    }
    
    console.log(`Audio processing for post ${postId} completed successfully`);
    if (res) {
      return res.json({
        success: true,
        message: 'Successfully processed audio',
        postId: postId,
        mp3Id: mp3File.$id,
        playlistId: playlist.$id,
        segmentCount: Object.values(segmentFileIds).length
      });
    }
    return res ? res.send() : undefined;
  } catch (error) {
    console.error('Error processing audio:', error);
    
    try {
      if (payload && payload.postId) {
        await databases.updateDocument(
          APPWRITE_DATABASE_ID,
          APPWRITE_COLLECTION_ID_POST,
          payload.postId,
          {
            processing_status: 'failed',
            processing_error: error.message,
            processing_completed_at: new Date().toISOString()
          }
        );
        console.log(`Updated post ${payload.postId} with error status`);
      }
    } catch (updateError) {
      console.error('Failed to update post with error status:', updateError);
    }
    
    if (res) {
      return res.json({
        success: false,
        message: 'Error processing audio',
        error: error.message
      });
    }
    return res ? res.send() : undefined;
  }
}; 