const { Client, Databases, Query, Storage, ID } = require('node-appwrite');
const fs = require('fs');
const path = require('path');
const { promisify } = require('util');
const writeFileAsync = promisify(fs.writeFile);
const readFileAsync = promisify(fs.readFile);
const unlinkAsync = promisify(fs.unlink);

// Initialize Appwrite client
const getClient = () => {
  const client = new Client();
  
  client
    .setEndpoint(process.env.APPWRITE_ENDPOINT)
    .setProject(process.env.APPWRITE_PROJECT_ID)
    .setKey(process.env.APPWRITE_API_KEY);
    
  return client;
};

// Initialize Appwrite services
const getDatabases = () => {
  return new Databases(getClient());
};

const getStorage = () => {
  return new Storage(getClient());
};

// Query functions
async function getUnprocessedPosts() {
  const databases = getDatabases();
  
  try {
    // Query for posts that have audio_url (WAV) but no mp3_url
    const posts = await databases.listDocuments(
      process.env.APPWRITE_DATABASE_ID,
      process.env.APPWRITE_COLLECTION_ID_POST,
      [
        Query.isNotNull('audio_url'),
        Query.isNull('mp3_url')
      ]
    );
    
    return posts.documents;
  } catch (error) {
    console.error('Error fetching unprocessed posts:', error);
    return [];
  }
}

// Download file from Appwrite Storage
async function downloadFile(fileId, localPath) {
  const storage = getStorage();
  
  try {
    const fileBlob = await storage.getFileDownload(
      process.env.APPWRITE_BUCKET_ID,
      fileId
    );
    
    const arrayBuffer = await fileBlob.arrayBuffer();
    const buffer = Buffer.from(arrayBuffer);
    
    await writeFileAsync(localPath, buffer);
    return localPath;
  } catch (error) {
    console.error(`Error downloading file ${fileId}:`, error);
    throw error;
  }
}

// Upload file to Appwrite Storage
async function uploadFile(localPath, fileName) {
  const storage = getStorage();
  
  try {
    const fileBuffer = await readFileAsync(localPath);
    
    const file = await storage.createFile(
      process.env.APPWRITE_BUCKET_ID,
      ID.unique(),
      fileBuffer,
      fileName
    );
    
    return file.$id;
  } catch (error) {
    console.error(`Error uploading file ${fileName}:`, error);
    throw error;
  }
}

// Update post with processed audio information
async function updatePostWithProcessedAudio(postId, data) {
  const databases = getDatabases();
  
  try {
    return await databases.updateDocument(
      process.env.APPWRITE_DATABASE_ID,
      process.env.APPWRITE_COLLECTION_ID_POST,
      postId,
      data
    );
  } catch (error) {
    console.error(`Error updating post ${postId}:`, error);
    throw error;
  }
}

module.exports = {
  getClient,
  getDatabases,
  getStorage,
  getUnprocessedPosts,
  downloadFile,
  uploadFile,
  updatePostWithProcessedAudio
}; 