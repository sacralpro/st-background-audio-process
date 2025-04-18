const path = require('path');
const fs = require('fs');
const { promisify } = require('util');
const readdirAsync = promisify(fs.readdir);
const statAsync = promisify(fs.stat);

const appwrite = require('./appwrite');
const audioProcessor = require('./audioProcessor');

// Process a single post with WAV audio
async function processPost(post) {
  console.log(`Processing post: ${post.$id} - ${post.trackname || 'Untitled'}`);
  
  try {
    // Обновляем статус обработки
    await appwrite.updatePostWithProcessedAudio(post.$id, {
      processing_status: 'processing'
    });
    
    // Extract WAV file ID from audio_url
    const audioUrl = post.audio_url;
    const wavFileId = audioUrl.split('/files/')[1].split('/')[0];
    
    // Create a unique name for the audio files based on post ID
    const baseName = `track_${post.$id}`;
    
    // Temporary file paths
    const tempDir = await audioProcessor.ensureTempDir();
    const wavPath = path.join(tempDir, `${baseName}.wav`);
    const mp3Filename = `${baseName}.mp3`;
    
    // Step 1: Download WAV file from Appwrite
    console.log(`Downloading WAV file: ${wavFileId}`);
    await appwrite.downloadFile(wavFileId, wavPath);
    
    // Step 1.5: Validate the audio file
    console.log(`Validating audio file: ${wavPath}`);
    try {
      const audioInfo = await audioProcessor.validateAudioFile(wavPath);
      console.log(`Audio validation successful: ${JSON.stringify(audioInfo)}`);
    } catch (validationError) {
      throw new Error(`Audio validation failed: ${validationError.message}`);
    }
    
    // Step 2: Convert WAV to MP3
    console.log(`Converting WAV to MP3 with 192k bitrate: ${wavPath}`);
    const mp3Path = await audioProcessor.convertWavToMp3(wavPath, mp3Filename);
    
    // Step 3: Upload MP3 to Appwrite
    console.log(`Uploading MP3 to Appwrite: ${mp3Path}`);
    const mp3FileId = await appwrite.uploadFile(mp3Path, mp3Filename);
    
    // Step 4: Create HLS segments and playlist (also with 192k bitrate)
    console.log(`Creating HLS segments with 192k bitrate for: ${mp3Path}`);
    const hlsResult = await audioProcessor.createHlsSegments(mp3Path, baseName);
    
    // Step 5: Upload segments to Appwrite
    console.log('Uploading HLS segments to Appwrite');
    const segmentsDir = hlsResult.segmentsDir;
    const segmentFiles = await readdirAsync(segmentsDir);
    
    const segmentFileIds = {};
    const streamingUrls = [];
    
    for (const segmentFile of segmentFiles) {
      if (segmentFile.endsWith('.ts')) {
        const segmentPath = path.join(segmentsDir, segmentFile);
        const segmentId = await appwrite.uploadFile(segmentPath, segmentFile);
        segmentFileIds[segmentFile] = segmentId;
        
        // Create streaming URL for each segment
        const streamingUrl = `${process.env.APPWRITE_ENDPOINT}/storage/buckets/${process.env.APPWRITE_BUCKET_ID}/files/${segmentId}/view?project=${process.env.APPWRITE_PROJECT_ID}`;
        streamingUrls.push(streamingUrl);
      }
    }
    
    // Step 6: Create a new m3u8 playlist with Appwrite URLs
    console.log('Generating Appwrite m3u8 playlist');
    const appwritePlaylistPath = await audioProcessor.generateAppwritePlaylist(
      hlsResult.playlistPath,
      segmentFileIds
    );
    
    // Step 7: Upload the playlist to Appwrite
    console.log('Uploading m3u8 playlist to Appwrite');
    const playlistFileId = await appwrite.uploadFile(appwritePlaylistPath, `${baseName}.m3u8`);
    
    // Step 8: Update the post with processed audio information
    console.log(`Updating post ${post.$id} with processed audio information`);
    await appwrite.updatePostWithProcessedAudio(post.$id, {
      mp3_url: `${process.env.APPWRITE_ENDPOINT}/storage/buckets/${process.env.APPWRITE_BUCKET_ID}/files/${mp3FileId}/view?project=${process.env.APPWRITE_PROJECT_ID}`,
      m3u8_url: `${process.env.APPWRITE_ENDPOINT}/storage/buckets/${process.env.APPWRITE_BUCKET_ID}/files/${playlistFileId}/view?project=${process.env.APPWRITE_PROJECT_ID}`,
      segments: JSON.stringify(Object.values(segmentFileIds)),
      streaming_urls: streamingUrls,
      processing_status: 'completed',
      processing_completed_at: new Date().toISOString()
    });
    
    // Step 9: Clean up temporary files
    console.log('Cleaning up temporary files');
    const filesToCleanup = [
      wavPath,
      mp3Path,
      appwritePlaylistPath,
      hlsResult.playlistPath,
      ...segmentFiles.map(file => path.join(segmentsDir, file))
    ];
    
    await audioProcessor.cleanupTempFiles(filesToCleanup);
    
    console.log(`Successfully processed post: ${post.$id}`);
    return true;
  } catch (error) {
    console.error(`Error processing post ${post.$id}:`, error);
    
    // Обновляем пост с информацией об ошибке
    try {
      await appwrite.updatePostWithProcessedAudio(post.$id, {
        processing_status: 'failed',
        processing_error: error.message
      });
    } catch (updateError) {
      console.error(`Failed to update post status: ${updateError.message}`);
    }
    
    return false;
  }
}

// Функция для обработки поста с повторными попытками
async function processPostWithRetry(post, maxRetries = 3) {
  let retries = 0;
  while (retries < maxRetries) {
    try {
      console.log(`Processing attempt ${retries + 1}/${maxRetries} for post ${post.$id}`);
      const result = await processPost(post);
      if (result) return true;
      
      // Если processPost вернул false, но не выбросил ошибку - увеличиваем счетчик
      retries++;
      
      if (retries < maxRetries) {
        const waitTime = 5000 * Math.pow(2, retries); // Экспоненциальное увеличение времени ожидания
        console.log(`Waiting ${waitTime/1000} seconds before next attempt...`);
        await new Promise(resolve => setTimeout(resolve, waitTime));
      }
    } catch (error) {
      console.error(`Attempt ${retries + 1} failed with error:`, error);
      retries++;
      
      if (retries < maxRetries) {
        const waitTime = 5000 * Math.pow(2, retries);
        console.log(`Waiting ${waitTime/1000} seconds before next attempt...`);
        await new Promise(resolve => setTimeout(resolve, waitTime));
      }
    }
  }
  
  console.error(`All ${maxRetries} attempts failed for post ${post.$id}`);
  
  // Обновляем пост с финальной информацией об ошибке
  try {
    await appwrite.updatePostWithProcessedAudio(post.$id, {
      processing_status: 'failed',
      processing_error: `Failed after ${maxRetries} attempts`
    });
  } catch (updateError) {
    console.error(`Failed to update final post status: ${updateError.message}`);
  }
  
  return false;
}

// Main processing function
async function processUnprocessedPosts() {
  try {
    console.log('Checking for unprocessed posts...');
    
    // Get all unprocessed posts
    const posts = await appwrite.getUnprocessedPosts();
    
    if (posts.length === 0) {
      console.log('No unprocessed posts found.');
      return;
    }
    
    console.log(`Found ${posts.length} unprocessed posts. Starting processing...`);
    
    // Process each post with retry mechanism
    for (const post of posts) {
      await processPostWithRetry(post);
    }
    
    console.log('Finished processing all posts.');
  } catch (error) {
    console.error('Error in main processing function:', error);
  }
}

module.exports = {
  processPost,
  processPostWithRetry,
  processUnprocessedPosts
}; 