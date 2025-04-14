// Appwrite Function template for audio processing
// This code can be used in an Appwrite Function to process audio files

const sdk = require('node-appwrite');
const ffmpeg = require('fluent-ffmpeg');
const fs = require('fs');
const path = require('path');
const { promisify } = require('util');
const writeFileAsync = promisify(fs.writeFile);
const readFileAsync = promisify(fs.readFile);
const unlinkAsync = promisify(fs.unlink);
const mkdirAsync = promisify(fs.mkdir);

// This is an Appwrite Function that can be deployed directly to Appwrite
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
    // Parse the function payload (you can set this when creating the function trigger)
    const payload = req.body || {};
    const { postId } = payload;
    
    if (!postId) {
      return res.json({
        success: false,
        message: 'Missing postId in payload'
      });
    }
    
    // Get the post from database
    const post = await databases.getDocument(
      APPWRITE_DATABASE_ID,
      APPWRITE_COLLECTION_ID_POST,
      postId
    );
    
    // Check if post needs processing
    if (!post.audio_url || post.mp3_url) {
      return res.json({
        success: true,
        message: 'Post already processed or has no audio file'
      });
    }
    
    // Create temp directory for processing
    const tempDir = '/tmp/audio-processing';
    await mkdirAsync(tempDir, { recursive: true });
    
    // Extract file ID from audio_url
    const audioUrl = post.audio_url;
    const wavFileId = audioUrl.split('/files/')[1].split('/')[0];
    
    // Create base name for files
    const baseName = `track_${post.$id}`;
    const wavPath = path.join(tempDir, `${baseName}.wav`);
    const mp3Path = path.join(tempDir, `${baseName}.mp3`);
    
    // Download WAV file
    const fileBlob = await storage.getFileDownload(APPWRITE_BUCKET_ID, wavFileId);
    const arrayBuffer = await fileBlob.arrayBuffer();
    const buffer = Buffer.from(arrayBuffer);
    await writeFileAsync(wavPath, buffer);
    
    // Convert WAV to MP3
    await new Promise((resolve, reject) => {
      ffmpeg(wavPath)
        .audioCodec('libmp3lame')
        .audioBitrate('192k')
        .format('mp3')
        .on('error', reject)
        .on('end', resolve)
        .save(mp3Path);
    });
    
    // Upload MP3 to Appwrite
    const mp3Buffer = await readFileAsync(mp3Path);
    const mp3File = await storage.createFile(
      APPWRITE_BUCKET_ID,
      sdk.ID.unique(),
      mp3Buffer,
      `${baseName}.mp3`
    );
    
    // Create HLS segments
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
        .on('error', reject)
        .on('end', resolve)
        .save(playlistPath);
    });
    
    // Upload segments and collect IDs
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
    
    // Create and upload modified playlist
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
    
    // Update post with processed audio information
    await databases.updateDocument(
      APPWRITE_DATABASE_ID,
      APPWRITE_COLLECTION_ID_POST,
      post.$id,
      {
        mp3_url: `${APPWRITE_ENDPOINT}/storage/buckets/${APPWRITE_BUCKET_ID}/files/${mp3File.$id}/view?project=${APPWRITE_FUNCTION_PROJECT_ID}`,
        m3u8_url: `${APPWRITE_ENDPOINT}/storage/buckets/${APPWRITE_BUCKET_ID}/files/${playlist.$id}/view?project=${APPWRITE_FUNCTION_PROJECT_ID}`,
        segments: JSON.stringify(Object.values(segmentFileIds)),
        streaming_urls: streamingUrls
      }
    );
    
    // Clean up
    await unlinkAsync(wavPath);
    await unlinkAsync(mp3Path);
    await unlinkAsync(newPlaylistPath);
    await unlinkAsync(playlistPath);
    for (const file of segmentFiles) {
      await unlinkAsync(path.join(segmentsDir, file));
    }
    
    return res.json({
      success: true,
      message: 'Successfully processed audio',
      mp3Id: mp3File.$id,
      playlistId: playlist.$id,
      segmentIds: Object.values(segmentFileIds)
    });
  } catch (error) {
    console.error('Error processing audio:', error);
    
    return res.json({
      success: false,
      message: 'Error processing audio',
      error: error.message
    });
  }
}; 