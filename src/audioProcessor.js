const ffmpeg = require('fluent-ffmpeg');
const path = require('path');
const fs = require('fs');
const { promisify } = require('util');
const mkdirAsync = promisify(fs.mkdir);
const existsAsync = promisify(fs.exists);
const unlinkAsync = promisify(fs.unlink);
const readFileAsync = promisify(fs.readFile);
const writeFileAsync = promisify(fs.writeFile);

// Ensure temporary directory exists
const ensureTempDir = async () => {
  const tempDir = path.join(__dirname, '../temp');
  if (!(await existsAsync(tempDir))) {
    await mkdirAsync(tempDir, { recursive: true });
  }
  return tempDir;
};

// Convert WAV to MP3
const convertWavToMp3 = async (wavPath, outputFilename) => {
  const tempDir = await ensureTempDir();
  const mp3Path = path.join(tempDir, outputFilename);
  
  return new Promise((resolve, reject) => {
    ffmpeg(wavPath)
      .audioCodec('libmp3lame')
      .audioBitrate('192k')
      .format('mp3')
      .on('error', (err) => {
        console.error('Error converting WAV to MP3:', err);
        reject(err);
      })
      .on('end', () => {
        console.log(`Successfully converted ${wavPath} to ${mp3Path}`);
        resolve(mp3Path);
      })
      .save(mp3Path);
  });
};

// Create HLS segments and playlist
const createHlsSegments = async (mp3Path, outputBasename) => {
  const tempDir = await ensureTempDir();
  const segmentsDir = path.join(tempDir, 'segments');
  
  // Ensure segments directory exists
  if (!(await existsAsync(segmentsDir))) {
    await mkdirAsync(segmentsDir, { recursive: true });
  }
  
  const segmentsPattern = path.join(segmentsDir, `${outputBasename}_%03d.ts`);
  const playlistPath = path.join(tempDir, `${outputBasename}.m3u8`);
  
  return new Promise((resolve, reject) => {
    ffmpeg(mp3Path)
      .audioCodec('aac')
      .audioBitrate('128k')
      .format('hls')
      .outputOptions([
        '-hls_time 10', // Each segment is 10 seconds long
        '-hls_list_size 0', // All segments in the playlist
        '-hls_segment_type mpegts', // Use MPEG-TS segments
        '-hls_segment_filename', segmentsPattern
      ])
      .on('error', (err) => {
        console.error('Error creating HLS segments:', err);
        reject(err);
      })
      .on('end', () => {
        console.log(`Successfully created HLS segments for ${mp3Path}`);
        resolve({
          playlistPath,
          segmentsDir,
          segmentsPattern
        });
      })
      .save(playlistPath);
  });
};

// Generate a new m3u8 playlist with Appwrite file IDs
const generateAppwritePlaylist = async (originalPlaylistPath, segmentFileIds) => {
  const tempDir = await ensureTempDir();
  const playlistContent = await readFileAsync(originalPlaylistPath, 'utf8');
  
  // Replace local segment references with Appwrite URLs
  let newPlaylistContent = playlistContent;
  
  Object.keys(segmentFileIds).forEach((segmentName) => {
    const fileId = segmentFileIds[segmentName];
    newPlaylistContent = newPlaylistContent.replace(
      new RegExp(segmentName, 'g'),
      `${process.env.APPWRITE_ENDPOINT}/storage/buckets/${process.env.APPWRITE_BUCKET_ID}/files/${fileId}/view?project=${process.env.APPWRITE_PROJECT_ID}`
    );
  });
  
  const newPlaylistPath = path.join(tempDir, 'appwrite_playlist.m3u8');
  await writeFileAsync(newPlaylistPath, newPlaylistContent, 'utf8');
  
  return newPlaylistPath;
};

// Clean up temporary files
const cleanupTempFiles = async (filePaths) => {
  try {
    for (const filePath of filePaths) {
      if (await existsAsync(filePath)) {
        await unlinkAsync(filePath);
        console.log(`Deleted temporary file: ${filePath}`);
      }
    }
  } catch (error) {
    console.error('Error cleaning up temporary files:', error);
  }
};

module.exports = {
  convertWavToMp3,
  createHlsSegments,
  generateAppwritePlaylist,
  cleanupTempFiles,
  ensureTempDir
}; 