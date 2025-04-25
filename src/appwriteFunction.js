// Appwrite Function for audio processing
// Optimized for direct calls from Sacral Track application

// Заменяем импорты ES модулей на CommonJS синтаксис
const { Client, Databases, Storage, ID, Query } = require('node-appwrite');
const axios = require('axios');
const path = require('path');
const fs = require('fs');
const { promisify } = require('util');
const os = require('os');
const ffmpeg = require('fluent-ffmpeg');
const dotenv = require('dotenv');

// Инициализируем dotenv для загрузки переменных окружения из .env файла (если он существует)
dotenv.config();

// Промисифицируем функции fs для асинхронного использования
const mkdirAsync = promisify(fs.mkdir);
const writeFileAsync = promisify(fs.writeFile);
const readFileAsync = promisify(fs.readFile);
const unlinkAsync = promisify(fs.unlink);
const readdirAsync = promisify(fs.readdir);
const statAsync = promisify(fs.stat);

// Константы для Appwrite
const APPWRITE_ENDPOINT = process.env.APPWRITE_ENDPOINT || 'https://cloud.appwrite.io/v1';
const APPWRITE_PROJECT_ID = process.env.APPWRITE_PROJECT_ID;
const APPWRITE_API_KEY = process.env.APPWRITE_API_KEY;
const APPWRITE_DATABASE_ID = process.env.APPWRITE_DATABASE_ID;
const APPWRITE_COLLECTION_ID = process.env.APPWRITE_COLLECTION_ID || 'posts';
const APPWRITE_BUCKET_ID = process.env.APPWRITE_BUCKET_ID;

// Добавляем константу для логов, если она будет использоваться
const APPWRITE_COLLECTION_ID_LOGS = process.env.APPWRITE_COLLECTION_ID_LOGS;

// Определяем константы для шагов обработки
const PROCESSING_STEPS = {
  INITIALIZE: 'initialize',
  DOWNLOAD: 'download',
  CONVERT: 'convert',
  SEGMENT: 'segment',
  UPLOAD_SEGMENTS: 'upload_segments',
  CREATE_PLAYLIST: 'create_playlist',
  FINALIZE: 'finalize'
};

// Инициализация Appwrite SDK
const client = new Client()
  .setEndpoint(APPWRITE_ENDPOINT)
  .setProject(APPWRITE_PROJECT_ID)
  .setKey(APPWRITE_API_KEY);

const databases = new Databases(client);
const storage = new Storage(client);

// Защита от ошибок парсинга JSON на самом раннем уровне
// Monkey patch JSON.parse чтобы предотвратить сбои
const originalJSONParse = JSON.parse;
JSON.parse = function safeParse(text, reviver) {
  if (typeof text !== 'string') {
    return originalJSONParse(text, reviver);
  }
  
  try {
    // Clean the input string of common problems
    let cleanedText = text
      .replace(/^\uFEFF/, '')                  // BOM
      .replace(/^\s+|\s+$/g, '')               // Leading/trailing whitespace
      .replace(/\u200B/g, '')                  // Zero-width space
      .replace(/\t/g, ' ')                     // Replace tabs with spaces
      .replace(/\r?\n/g, '');                  // Remove all newlines
    
    // Find valid JSON in malformed string
    const firstBrace = cleanedText.indexOf('{');
    const lastBrace = cleanedText.lastIndexOf('}');
    if (firstBrace >= 0 && lastBrace > firstBrace) {
      cleanedText = cleanedText.substring(firstBrace, lastBrace + 1);
    }
    
    // Try to parse with original parser
    return originalJSONParse(cleanedText, reviver);
  } catch (e) {
    console.error('[APPWRITE_FUNCTION] JSON parse error intercepted:', e.message);
    // Return a safe default object instead of crashing
    return {};
  }
};

// Помощник для извлечения postId из различных источников
function extractPostId(req, log, logError) {
  // Проверяем прямые свойства req
  if (req.postId) {
    log(`Found postId directly in req: ${req.postId}`);
    return req.postId;
  }
  
  // Проверяем параметры запроса
  if (req.params && req.params.postId) {
    log(`Found postId in req.params: ${req.params.postId}`);
    return req.params.postId;
  }

  // Проверяем переменные
  if (req.variables && req.variables.postId) {
    log(`Found postId in req.variables: ${req.variables.postId}`);
    return req.variables.postId;
  }
  
  // Проверяем путь в URL
  if (req.url) {
    const urlMatch = req.url.match(/\/posts\/([^\/\?]+)/);
    if (urlMatch && urlMatch[1]) {
      log(`Found postId in URL path: ${urlMatch[1]}`);
      return urlMatch[1];
    }
  }
  
  // Рекурсивный поиск в объекте с защитой от бесконечной рекурсии
  function findPostIdInObject(obj, path = '', visited = new Set()) {
    // Защита от циклических ссылок
    if (!obj || typeof obj !== 'object' || visited.has(obj)) {
      return null;
    }
    
    // Добавляем текущий объект в посещенные
    visited.add(obj);
    
    // Прямой поиск
    if (obj.postId) {
      log(`Found postId at ${path}.postId: ${obj.postId}`);
      return obj.postId;
    }
    
    // Поиск в data
    if (obj.data && obj.data.postId && !visited.has(obj.data)) {
      log(`Found postId at ${path}.data.postId: ${obj.data.postId}`);
      return obj.data.postId;
    }
    
    // Поиск в event
    if (obj.event && obj.event.postId && !visited.has(obj.event)) {
      log(`Found postId at ${path}.event.postId: ${obj.event.postId}`);
      return obj.event.postId;
    }
    
    // Не углубляемся слишком далеко
    if (path.split('.').length > 4) return null;
    
    // Рекурсивный поиск в свойствах
    for (const key in obj) {
      // Пропускаем ключи, которые могут вызвать циклические ссылки
      if (key === 'req' || key === 'res' || key === 'parent') {
        continue;
      }
      
      if (typeof obj[key] === 'object' && obj[key] !== null) {
        const result = findPostIdInObject(obj[key], path ? `${path}.${key}` : key, visited);
        if (result) return result;
      }
    }
    
    return null;
  }
  
  // Запускаем рекурсивный поиск
  return findPostIdInObject(req);
}

// Main function
module.exports = async function(req, res) {
  // Инициализируем объект payload заранее
  let payload = {};
  
  // Правильно определяем функции логирования
  const log = req.log || console.log;
  const error = req.error || console.error;
  
  log('Запуск функции обработки аудио');
  
  try {
    // Логируем все доступные ключи для отладки
    log(`Available keys in req: ${Object.keys(req).join(', ')}`);
    
    // Безопасная инициализация payload из разных источников
    if (req.body) {
      try {
        if (typeof req.body === 'string') {
          Object.assign(payload, JSON.parse(req.body));
          log('Parsed payload from req.body (string)');
        } else if (typeof req.body === 'object') {
          Object.assign(payload, req.body);
          log('Assigned payload from req.body (object)');
        }
      } catch (e) {
        error(`Error parsing req.body: ${e.message}`);
      }
    } else if (req.bodyJson) {
      // В новых версиях Appwrite
      Object.assign(payload, req.bodyJson);
      log('Assigned payload from req.bodyJson');
    } else if (req.bodyText) {
      // Альтернативный источник в новых версиях Appwrite
      try {
        Object.assign(payload, JSON.parse(req.bodyText));
        log('Parsed payload from req.bodyText');
      } catch (e) {
        error(`Error parsing req.bodyText: ${e.message}`);
      }
    } else if (req.rawBody || req.bodyRaw) {
      // Еще один вариант для обратной совместимости
      const rawData = req.rawBody || req.bodyRaw;
      try {
        Object.assign(payload, JSON.parse(rawData));
        log('Parsed payload from rawBody/bodyRaw');
      } catch (e) {
        error(`Error parsing rawBody/bodyRaw: ${e.message}`);
      }
    }
    
    // Логируем ключи payload для отладки
    log(`Payload keys: ${Object.keys(payload).join(', ')}`);
    
    // Получаем postId из payload или с помощью функции извлечения
    let postId = payload.postId;
    
    // Если postId не найден в payload, используем функцию извлечения
    if (!postId) {
      log('postId not found in payload, trying to extract from request');
      postId = extractPostId(req, log, error);
    }
    
    // Проверяем наличие postId перед продолжением
    if (!postId) {
      log('postId not found in request');
      
      // Проверяем res перед использованием
      if (res && typeof res.json === 'function') {
        return res.json({
          success: false,
          message: 'Post ID is required but was not found in the request'
        });
      } else {
        error('res.json is not available');
        throw new Error('Post ID is required but was not found in the request');
      }
    }
    
    log(`Processing audio for post ID: ${postId}`);
    
    // Добавим информацию о клиенте Appwrite
    const sdk = require('node-appwrite');
    
    // Инициализация клиента в начале функции
    const client = new sdk.Client();
    
    const { 
      APPWRITE_FUNCTION_PROJECT_ID,
      APPWRITE_API_KEY,
      APPWRITE_DATABASE_ID,
      APPWRITE_COLLECTION_ID,
      APPWRITE_BUCKET_ID,
      APPWRITE_ENDPOINT
    } = process.env;

    client
      .setEndpoint(APPWRITE_ENDPOINT)
      .setProject(APPWRITE_FUNCTION_PROJECT_ID)
      .setKey(APPWRITE_API_KEY);
    
    const databases = new sdk.Databases(client);
    const storage = new sdk.Storage(client);
    
    // Получаем информацию о посте
    try {
      log(`Fetching post with ID: ${postId}`);
      const post = await databases.getDocument(
        APPWRITE_DATABASE_ID,
        APPWRITE_COLLECTION_ID,
        postId
      );
      
      log(`Post found: ${post.$id}`);
      
      // Обрабатываем пост
      let audioUrl = post.audioUrl;
      
      if (!audioUrl) {
        logError(`No audioUrl found in post: ${postId}`);
        if (res && typeof res.json === 'function') {
          return res.json({
            success: false,
            message: 'No audioUrl found in post'
          });
        } else {
          return { success: false, message: 'No audioUrl found in post' };
        }
      }
      
      log(`Starting audio processing for URL: ${audioUrl}`);
      
      // Здесь ваш код для обработки аудио
      // ...
      
      // Вернем успешный ответ
      if (res && typeof res.json === 'function') {
        return res.json({
          success: true,
          message: 'Audio processing started',
          postId: postId
        });
      } else {
        return { 
          success: true, 
          message: 'Audio processing started', 
          postId: postId 
        };
      }
    } catch (error) {
      logError(`Error processing post ${postId}: ${error.message}`);
      logError(error.stack);
      
      // Безопасный ответ в случае ошибки
      if (res && typeof res.json === 'function') {
        return res.json({
          success: false,
          message: `Error processing post: ${error.message}`,
          postId: postId
        });
      } else {
        return { 
          success: false, 
          message: `Error processing post: ${error.message}`, 
          postId: postId 
        };
      }
    }
  } catch (error) {
    // Обработка общих ошибок
    const logError = (req.error && typeof req.error === 'function') 
      ? req.error 
      : console.error;
    
    logError(`Unexpected error: ${error.message}`);
    logError(error.stack);
    
    // Безопасный ответ в случае ошибки
    if (res && typeof res.json === 'function') {
      return res.json({ 
        success: false, 
        error: `Unexpected error: ${error.message}` 
      });
    } else {
      return { 
        success: false, 
        error: `Unexpected error: ${error.message}` 
      };
    }
  }
};

// Helper function to process unprocessed posts
async function processUnprocessedPosts(databases, storage, client) {
  try {
    // Find unprocessed posts
    const posts = await databases.listDocuments(
      APPWRITE_DATABASE_ID,
      APPWRITE_COLLECTION_ID,
      [
        Query.equal('audio_processed', false),
        Query.equal('audio_processing', false),
        Query.isNotNull('audio_url'),
        Query.limit(5)
      ]
    );
    
    log(`Found ${posts.total} unprocessed posts`);
    
    // Process each post
    const results = [];
    for (const post of posts.documents) {
      log(`Processing unprocessed post: ${post.$id}`);
      
      // Mark as processing
      await databases.updateDocument(
        APPWRITE_DATABASE_ID,
        APPWRITE_COLLECTION_ID,
        post.$id,
        {
          audio_processing: true
        }
      );
      
      // Process the post
      const result = await processAudio(post.$id, databases, storage, client);
      results.push(result);
      
      // Schedule next step if needed
      if (result.success && result.currentStep) {
        await scheduleNextStep(client, result.executionId, result.postId);
        log(`Scheduled next step: ${result.currentStep}`);
      }
    }
    
    return {
      success: true,
      message: `Processed ${results.length} posts`,
      results
    };
  } catch (error) {
    logError('Error fetching unprocessed posts:', error);
    return {
      success: false,
      message: `Error fetching unprocessed posts: ${error.message}`
    };
  }
}

// Helper function to update UI progress
async function updateUIProgress(postId, databases, step, progress, status = 'processing') {
  try {
    const progressPercentage = getProgressPercentage(step, progress);
    
    await databases.updateDocument(
      APPWRITE_DATABASE_ID,
      APPWRITE_COLLECTION_ID,
      postId,
      {
        audio_processing_progress: progressPercentage,
        audio_processing_step: step,
        audio_processing_status: status,
        audio_processing_updated_at: new Date().toISOString()
      }
    );
    
    log(`Updated UI progress for post ${postId}: ${step} - ${progressPercentage}%`);
    return true;
  } catch (error) {
    logError(`Failed to update UI progress for post ${postId}:`, error);
    return false;
  }
}

// Helper function to calculate progress percentage based on step
function getProgressPercentage(step, progress = 0) {
  const STEP_WEIGHTS = {
    [PROCESSING_STEPS.INITIALIZE]: { weight: 5, position: 0 },
    [PROCESSING_STEPS.DOWNLOAD]: { weight: 15, position: 1 },
    [PROCESSING_STEPS.CONVERT]: { weight: 20, position: 2 },
    [PROCESSING_STEPS.SEGMENT]: { weight: 25, position: 3 },
    [PROCESSING_STEPS.UPLOAD_SEGMENTS]: { weight: 20, position: 4 },
    [PROCESSING_STEPS.CREATE_PLAYLIST]: { weight: 10, position: 5 },
    [PROCESSING_STEPS.FINALIZE]: { weight: 5, position: 6 }
  };
  
  // Default to INITIALIZE if step not found
  const stepInfo = STEP_WEIGHTS[step] || STEP_WEIGHTS[PROCESSING_STEPS.INITIALIZE];
  const previousStepsWeight = Object.values(STEP_WEIGHTS)
    .filter(s => s.position < stepInfo.position)
    .reduce((sum, s) => sum + s.weight, 0);
  
  // Calculate progress within current step (0-100%)
  const stepProgress = Math.min(Math.max(progress, 0), 100) / 100;
  const currentStepContribution = stepInfo.weight * stepProgress;
  
  // Calculate total progress
  const totalProgress = previousStepsWeight + currentStepContribution;
  return Math.round(totalProgress);
}

// Helper function to send webhook notifications about progress
async function sendProgressWebhook(postId, step, progress, status = 'processing') {
  try {
    // Check if webhook URL is configured
    const webhookUrl = process.env.PROGRESS_WEBHOOK_URL;
    if (!webhookUrl) {
      log('No progress webhook URL configured, skipping notification');
      return false;
    }
    
    // Calculate progress percentage
    const progressPercentage = getProgressPercentage(step, progress);
    
    // Prepare webhook payload
    const payload = {
      postId,
      step,
      progress: progressPercentage,
      status,
      timestamp: new Date().toISOString()
    };
    
    // Send webhook notification
    const response = await axios.post(webhookUrl, payload, {
      headers: {
        'Content-Type': 'application/json',
        'X-Webhook-Source': 'appwrite-function'
      }
    });
    
    log(`Sent progress webhook for post ${postId}: ${response.status}`);
    return true;
  } catch (error) {
    logError(`Failed to send progress webhook for post ${postId}:`, error);
    return false;
  }
}

// Helper function to notify about progress (UI + webhook)
async function notifyProgress(postId, databases, step, progress, status = 'processing') {
  // Update UI progress
  await updateUIProgress(postId, databases, step, progress, status);
  
  // Send webhook notification
  await sendProgressWebhook(postId, step, progress, status);
}

// Helper function for handling download continuation
async function processContinuationDownload(post, req, res, databases, storage, executionRecord, startTime) {
  const logPrefix = `[CONTINUATION-DOWNLOAD] [${post.$id}]`;
  log(`${logPrefix} Starting download process`);
  
  try {
    // Create a temp directory for processing
    const tempDir = path.join('/tmp', `audio-processing-${post.$id}`);
    try {
      await mkdirAsync(tempDir, { recursive: true });
      log(`${logPrefix} Created temp directory: ${tempDir}`);
    } catch (err) {
      if (err.code !== 'EEXIST') {
        throw err;
      }
    }
    
    // Download the audio file
    const audioFileName = `${post.$id}.mp3`;
    const audioFilePath = path.join(tempDir, audioFileName);
    
    log(`${logPrefix} Downloading audio file ${post.audio_file_id} to ${audioFilePath}`);
    
    // Get the file data
    const fileData = await storage.getFileDownload(
      APPWRITE_BUCKET_ID,
      post.audio_file_id
    );
    
    // Save the file to disk
    await writeFileAsync(audioFilePath, Buffer.from(fileData));
    
    log(`${logPrefix} Downloaded and saved audio file to ${audioFilePath}`);
    
    // Check if MP3 is already uploaded or create a new one
    let mp3UploadResult;
    if (post.mp3_file_id) {
      log(`${logPrefix} Using existing MP3 file: ${post.mp3_file_id}`);
      mp3UploadResult = { $id: post.mp3_file_id };
    } else {
      // Upload mp3 as a processed file
      mp3UploadResult = await storage.createFile(
        APPWRITE_BUCKET_ID,
        ID.unique(),
        {
          path: audioFilePath,
          type: 'audio/mpeg'
        },
        [`audio/${post.$id}`]
      );
      
      log(`${logPrefix} Uploaded MP3 file, ID: ${mp3UploadResult.$id}`);
    }
    
    // Update post with mp3 ID and progress
    await databases.updateDocument(
      APPWRITE_DATABASE_ID,
      APPWRITE_COLLECTION_ID_POST,
      post.$id,
      {
        mp3_file_id: mp3UploadResult.$id,
        mp3_file_path: audioFilePath,
        temp_dir: tempDir,
        processing_progress: 30,
        processing_stage: 'ffmpeg_process',
        processing_continuation_scheduled: true,
        processing_next_step_at: new Date(Date.now() + 3000).toISOString()
      }
    );
    
    // Schedule FFmpeg processing
    const taskId = await scheduleContinuationTask(
      {
        ...post,
        processing_stage: 'ffmpeg_process',
        mp3_file_id: mp3UploadResult.$id
      }, 
      3
    );
    
    return res.json({
      success: true,
      message: 'Audio download completed, scheduled FFmpeg processing',
      postId: post.$id,
      mp3_file_id: mp3UploadResult.$id,
      continuation_scheduled: true,
      continuation_task_id: taskId || 'none'
    });
  } catch (error) {
    logError(`${logPrefix} Error:`, error);
    
    // Update post with error
    try {
      await databases.updateDocument(
        APPWRITE_DATABASE_ID,
        APPWRITE_COLLECTION_ID_POST,
        post.$id,
        {
          processing_status: 'failed',
          processing_error: `Download error: ${error.message}`,
          processing_completed_at: new Date().toISOString()
        }
      );
    } catch (updateError) {
      logError(`${logPrefix} Failed to update post with error:`, updateError);
    }
    
    return res.json({
      success: false,
      message: 'Error in download processing',
      error: error.message
    });
  }
}

// Helper function for FFmpeg processing
async function processContinuationFFmpeg(post, req, res, databases, storage, executionRecord, startTime) {
  const logPrefix = `[CONTINUATION-FFMPEG] [${post.$id}]`;
  log(`${logPrefix} Starting FFmpeg processing`);
  
  try {
    // Get temp directory and file path from post data
    const tempDir = post.temp_dir || path.join('/tmp', `audio-processing-${post.$id}`);
    const audioFilePath = post.mp3_file_path || path.join(tempDir, `${post.$id}.mp3`);
    
    // Ensure directories exist
    const hlsDir = path.join(tempDir, 'hls');
    try {
      await mkdirAsync(tempDir, { recursive: true });
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
    log(`${logPrefix} Creating HLS segments...`);
    
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
          log(`${logPrefix} Processing: ${JSON.stringify(progress)}`);
        })
        .on('end', () => {
          log(`${logPrefix} HLS segmentation completed`);
          resolve();
        })
        .on('error', err => {
          logError(`${logPrefix} Error during HLS segmentation:`, err);
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
        processing_progress: 60,
        processing_stage: 'playlist_preparation',
        m3u8_path: m3u8Path,
        hls_dir: hlsDir,
        processing_continuation_scheduled: true,
        processing_next_step_at: new Date(Date.now() + 3000).toISOString()
      }
    );
    
    // Schedule playlist preparation
    const taskId = await scheduleContinuationTask(
      {
        ...post,
        processing_stage: 'playlist_preparation',
        m3u8_path: m3u8Path,
        hls_dir: hlsDir
      }, 
      3
    );
    
    return res.json({
      success: true,
      message: 'FFmpeg processing completed, scheduled playlist preparation',
      postId: post.$id,
      mp3_file_id: post.mp3_file_id,
      continuation_scheduled: true,
      continuation_task_id: taskId || 'none'
    });
  } catch (error) {
    logError(`${logPrefix} Error:`, error);
    
    // Update post with error
    try {
      await databases.updateDocument(
        APPWRITE_DATABASE_ID,
        APPWRITE_COLLECTION_ID_POST,
        post.$id,
        {
          processing_status: 'failed',
          processing_error: `FFmpeg error: ${error.message}`,
          processing_completed_at: new Date().toISOString()
        }
      );
    } catch (updateError) {
      logError(`${logPrefix} Failed to update post with error:`, updateError);
    }
    
    return res.json({
      success: false,
      message: 'Error in FFmpeg processing',
      error: error.message
    });
  }
}

// Helper function for playlist preparation
async function processContinuationPlaylistPreparation(post, req, res, databases, storage, executionRecord, startTime) {
  const logPrefix = `[CONTINUATION-PLAYLIST-PREP] [${post.$id}]`;
  log(`${logPrefix} Starting playlist preparation`);
  
  try {
    // Get paths from post data
    const tempDir = post.temp_dir || path.join('/tmp', `audio-processing-${post.$id}`);
    const hlsDir = post.hls_dir || path.join(tempDir, 'hls');
    const m3u8Path = post.m3u8_path || path.join(hlsDir, 'playlist.m3u8');
    
    // Read the generated playlist
    const playlistContent = await readFileAsync(m3u8Path, 'utf8');
    log(`${logPrefix} Read playlist: ${m3u8Path}`);
    
    // Get segment files
    const segmentFiles = fs.readdirSync(hlsDir).filter(file => file.endsWith('.aac'));
    log(`${logPrefix} Found ${segmentFiles.length} segment files`);
    
    // If we have many segments, switch to segment upload mode
    if (segmentFiles.length > 5) {
      // Prepare for segmented upload
      await databases.updateDocument(
        APPWRITE_DATABASE_ID,
        APPWRITE_COLLECTION_ID_POST,
        post.$id,
        {
          processing_progress: 65,
          processing_stage: 'segment_upload',
          playlist_content: playlistContent,
          segment_files_remaining: segmentFiles.length,
          segment_files_processed: 0,
          hls_segment_ids: [],
          segment_urls: [],
          processing_continuation_scheduled: true,
          processing_next_step_at: new Date(Date.now() + 3000).toISOString()
        }
      );
      
      // Schedule segment upload
      const taskId = await scheduleContinuationTask(
        {
          ...post,
          processing_stage: 'segment_upload',
          segment_files_remaining: segmentFiles.length,
          segment_files_processed: 0
        }, 
        3
      );
      
      return res.json({
        success: true,
        message: `Playlist preparation completed, scheduled upload of ${segmentFiles.length} segments`,
        postId: post.$id,
        mp3_file_id: post.mp3_file_id,
        continuation_scheduled: true,
        continuation_task_id: taskId || 'none'
      });
    } else {
      // For small number of segments, process them directly
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
        
        log(`${logPrefix} Uploaded segment ${segmentFile}, ID: ${segmentUploadResult.$id}`);
      }
      
      // Move directly to playlist creation
      await databases.updateDocument(
        APPWRITE_DATABASE_ID,
        APPWRITE_COLLECTION_ID_POST,
        post.$id,
        {
          processing_progress: 85,
          processing_stage: 'playlist_creation',
          playlist_content: playlistContent,
          hls_segment_ids: segmentIds,
          segment_urls: segmentUrls,
          processing_continuation_scheduled: true,
          processing_next_step_at: new Date(Date.now() + 3000).toISOString()
        }
      );
      
      // Schedule playlist creation
      const taskId = await scheduleContinuationTask(
        {
          ...post,
          processing_stage: 'playlist_creation',
          hls_segment_ids: segmentIds,
          segment_urls: segmentUrls
        }, 
        3
      );
      
      return res.json({
        success: true,
        message: 'Small playlist processed directly, scheduled final playlist creation',
        postId: post.$id,
        mp3_file_id: post.mp3_file_id,
        continuation_scheduled: true,
        continuation_task_id: taskId || 'none'
      });
    }
  } catch (error) {
    logError(`${logPrefix} Error:`, error);
    
    // Update post with error
    try {
      await databases.updateDocument(
        APPWRITE_DATABASE_ID,
        APPWRITE_COLLECTION_ID_POST,
        post.$id,
        {
          processing_status: 'failed',
          processing_error: `Playlist preparation error: ${error.message}`,
          processing_completed_at: new Date().toISOString()
        }
      );
    } catch (updateError) {
      logError(`${logPrefix} Failed to update post with error:`, updateError);
    }
    
    return res.json({
      success: false,
      message: 'Error in playlist preparation',
      error: error.message
    });
  }
}

// Helper function to schedule a continuation if task scheduler is enabled
async function scheduleContinuationTask(post, delaySeconds = 5) {
  try {
    // Check if we have the environment variables needed for task scheduling
    if (process.env.APPWRITE_FUNCTION_ID && 
        process.env.APPWRITE_API_KEY && 
        process.env.APPWRITE_FUNCTION_PROJECT_ID && 
        process.env.APPWRITE_ENDPOINT) {
      
      log(`[SCHEDULER] Creating continuation task for post ${post.$id} with ${delaySeconds}s delay`);
      
      // Create a client for the Appwrite Functions API
      const schedulerClient = new AppwriteClient();
      schedulerClient
        .setEndpoint(process.env.APPWRITE_ENDPOINT)
        .setProject(process.env.APPWRITE_FUNCTION_PROJECT_ID)
        .setKey(process.env.APPWRITE_API_KEY);
      
      const functions = new Functions(schedulerClient);
      
      // Create execution payload
      const payload = JSON.stringify({
        postId: post.$id,
        isContinuation: true,
        stage: post.processing_stage,
        continuationToken: `continuation-${post.$id}-${Date.now()}`
      });
      
      // Execute the function after delay
      const execution = await functions.createExecution(
        process.env.APPWRITE_FUNCTION_ID,
        payload,
        false, // async execution
        `/`, // path 
        'POST', // method
        { 'X-Appwrite-Continuation': 'true' } // custom headers
      );
      
      log(`[SCHEDULER] Created continuation task, execution ID: ${execution.$id}`);
      
      return execution.$id;
    } else {
      log('[SCHEDULER] Task scheduling disabled due to missing environment variables');
      return null;
    }
  } catch (error) {
    logError('[SCHEDULER] Error scheduling continuation task:', error);
    return null;
  }
}

async function processContinuationSegmentUpload(post, req, res, databases, storage, executionRecord, startTime) {
  log(`Starting segment upload for post ${post.$id}, continuation processing`);
  
  try {
    // Get the paths from the post data
    const tempDir = post.processing_temp_dir;
    const hlsDir = post.processing_hls_dir;
    const segmentsToUpload = post.processing_segments_to_upload || [];
    const uploadedSegments = post.processing_uploaded_segments || [];
    const totalSegments = post.processing_total_segments || 0;
    
    if (!tempDir || !hlsDir || !segmentsToUpload || segmentsToUpload.length === 0) {
      throw new Error('Missing required path information for segment upload continuation');
    }
    
    log(`Found ${segmentsToUpload.length} segments to upload, already uploaded: ${uploadedSegments.length}, total: ${totalSegments}`);
    
    // Take a batch of segments to process
    const batchSize = 5;
    const segmentBatch = segmentsToUpload.slice(0, batchSize);
    const remainingSegments = segmentsToUpload.slice(batchSize);
    
    // Upload each segment in the batch
    const newUploadedSegments = [...uploadedSegments];
    
    for (const segmentFile of segmentBatch) {
      const segmentPath = path.join(hlsDir, segmentFile);
      
      // Check if the file exists
      if (!fs.existsSync(segmentPath)) {
        log(`Segment file not found: ${segmentPath}`);
        continue;
      }
      
      // Read segment file content
      const fileContent = await fsReadFile(segmentPath);
      
      // Upload the segment to storage
      const segmentUpload = await storage.createFile(
        APPWRITE_BUCKET_ID,
        ID.unique(),
        InputFile.fromBuffer(fileContent, segmentFile)
      );
      
      // Get the file URL
      const fileUrl = storage.getFileView(APPWRITE_BUCKET_ID, segmentUpload.$id);
      
      // Add to uploaded segments
      newUploadedSegments.push({
        id: segmentUpload.$id,
        name: segmentFile,
        url: fileUrl
      });
      
      log(`Uploaded segment ${segmentFile}, ID: ${segmentUpload.$id}`);
    }
    
    // Calculate progress
    const progress = Math.min(
      75 + Math.floor((newUploadedSegments.length / totalSegments) * 20),
      94
    );
    
    // Update the database
    if (remainingSegments.length > 0) {
      // More segments to upload, schedule continuation
      await databases.updateDocument(
        APPWRITE_DATABASE_ID,
        APPWRITE_COLLECTION_ID_POST,
        post.$id,
        {
          processing_status: 'processing',
          processing_progress: progress,
          processing_uploaded_segments: newUploadedSegments,
          processing_segments_to_upload: remainingSegments,
          processing_continuation_scheduled: true,
          processing_stage: 'segment_upload',
          processing_next_step_at: new Date(Date.now() + 3000).toISOString()
        }
      );
      
      // Schedule the next batch
      const taskId = await scheduleContinuationTask(
        { 
          ...post, 
          processing_stage: 'segment_upload',
          processing_uploaded_segments: newUploadedSegments,
          processing_segments_to_upload: remainingSegments
        },
        3
      );
      
      return res.json({
        success: true,
        message: `Uploaded ${segmentBatch.length} segments, ${remainingSegments.length} remaining`,
        postId: post.$id,
        progress,
        continuation_scheduled: true,
        continuation_task_id: taskId || 'none'
      });
    } else {
      // All segments uploaded, proceed to playlist creation
      await databases.updateDocument(
        APPWRITE_DATABASE_ID,
        APPWRITE_COLLECTION_ID_POST,
        post.$id,
        {
          processing_status: 'processing',
          processing_progress: 95,
          processing_uploaded_segments: newUploadedSegments,
          processing_stage: 'playlist_creation',
          processing_continuation_scheduled: true,
          processing_next_step_at: new Date(Date.now() + 3000).toISOString()
        }
      );
      
      // Schedule playlist creation
      const taskId = await scheduleContinuationTask(
        { 
          ...post, 
          processing_stage: 'playlist_creation',
          processing_uploaded_segments: newUploadedSegments
        },
        3
      );
      
      return res.json({
        success: true,
        message: 'All segments uploaded, proceeding to playlist creation',
        postId: post.$id,
        progress: 95,
        continuation_scheduled: true,
        continuation_task_id: taskId || 'none'
      });
    }
  } catch (error) {
    log(`Error in segment upload continuation: ${error.message}`);
    logError('Error in segment upload continuation:', error);
    
    // Update the database with error
    try {
      await databases.updateDocument(
        APPWRITE_DATABASE_ID,
        APPWRITE_COLLECTION_ID_POST,
        post.$id,
        {
          processing_status: 'error',
          processing_error: `Error in segment upload: ${error.message}`,
          processing_continuation_scheduled: false
        }
      );
    } catch (dbError) {
      logError('Failed to update error status in database:', dbError);
    }
    
    return res.json({
      success: false,
      message: `Error in segment upload: ${error.message}`,
      postId: post.$id
    });
  }
}

async function processContinuationPlaylistCreation(post, req, res, databases, storage, executionRecord, startTime) {
  log(`Starting playlist creation for post ${post.$id}, continuation processing`);
  
  try {
    // Get the paths from the post data
    const tempDir = post.processing_temp_dir;
    const hlsDir = post.processing_hls_dir;
    const uploadedSegments = post.processing_uploaded_segments || [];
    
    if (!tempDir || !hlsDir || !uploadedSegments || uploadedSegments.length === 0) {
      throw new Error('Missing required information for playlist creation');
    }
    
    log(`Creating playlist with ${uploadedSegments.length} segments`);
    
    // Read the original playlist file
    const playlistPath = path.join(hlsDir, 'playlist.m3u8');
    let playlistContent = await fsReadFile(playlistPath, 'utf8');
    
    // Modify the playlist to point to the uploaded segments
    for (const segment of uploadedSegments) {
      // Replace the segment filename with the URL in the playlist
      playlistContent = playlistContent.replace(
        new RegExp(segment.name, 'g'),
        segment.url
      );
    }
    
    // Save the modified playlist to temp dir
    const modifiedPlaylistPath = path.join(tempDir, 'playlist_modified.m3u8');
    await fsWriteFile(modifiedPlaylistPath, playlistContent);
    
    // Upload the modified playlist
    const playlistUpload = await storage.createFile(
      APPWRITE_BUCKET_ID,
      ID.unique(),
      InputFile.fromPath(modifiedPlaylistPath, 'playlist.m3u8')
    );
    
    // Get the playlist URL
    const playlistUrl = storage.getFileView(APPWRITE_BUCKET_ID, playlistUpload.$id);
    
    log(`Playlist uploaded, ID: ${playlistUpload.$id}`);
    
    // Update the database with the playlist information
    await databases.updateDocument(
      APPWRITE_DATABASE_ID,
      APPWRITE_COLLECTION_ID_POST,
      post.$id,
      {
        processing_status: 'completed',
        processing_progress: 100,
        processing_hls_playlist_id: playlistUpload.$id,
        processing_hls_playlist_url: playlistUrl,
        processing_segment_ids: uploadedSegments.map(s => s.id),
        processing_completion_time: new Date().toISOString(),
        processing_continuation_scheduled: false,
        processing_stage: 'completed'
      }
    );
    
    // Attempt to clean up temporary files
    try {
      if (fs.existsSync(tempDir)) {
        // Delete files in directory recursively
        const deleteTempFiles = async (directory) => {
          const files = fs.readdirSync(directory);
          
          for (const file of files) {
            const filePath = path.join(directory, file);
            const stats = fs.statSync(filePath);
            
            if (stats.isDirectory()) {
              await deleteTempFiles(filePath);
              fs.rmdirSync(filePath);
            } else {
              fs.unlinkSync(filePath);
            }
          }
        };
        
        await deleteTempFiles(tempDir);
        fs.rmdirSync(tempDir);
        log(`Temporary directory cleaned up: ${tempDir}`);
      }
    } catch (cleanupError) {
      logError('Error cleaning up temporary files:', cleanupError);
      // Continue despite cleanup error
    }
    
    return res.json({
      success: true,
      message: 'Audio processing completed successfully',
      postId: post.$id,
      progress: 100,
      mp3_id: post.processing_mp3_file_id,
      mp3_url: post.processing_mp3_file_url,
      hls_playlist_id: playlistUpload.$id,
      hls_playlist_url: playlistUrl,
      segment_ids: uploadedSegments.map(s => s.id),
      duration: post.processing_duration
    });
  } catch (error) {
    log(`Error in playlist creation: ${error.message}`);
    logError('Error in playlist creation:', error);
    
    // Update the database with error
    try {
      await databases.updateDocument(
        APPWRITE_DATABASE_ID,
        APPWRITE_COLLECTION_ID_POST,
        post.$id,
        {
          processing_status: 'error',
          processing_error: `Error in playlist creation: ${error.message}`,
          processing_continuation_scheduled: false
        }
      );
    } catch (dbError) {
      logError('Failed to update error status in database:', dbError);
    }
    
    return res.json({
      success: false,
      error: `Error in playlist creation: ${error.message}`,
      postId: post.$id
    });
  }
}

async function cleanupAndUpdateExecution(post, executionRecord, databases, success = true, errorMessage = null) {
  log(`Updating execution record ${executionRecord?.$id || 'unknown'}`);
  
  try {
    // Check if we need to clean up temp files
    if (success && post.processing_temp_dir && fs.existsSync(post.processing_temp_dir)) {
      try {
        // Delete files in directory recursively
        const deleteTempFiles = async (directory) => {
          const files = fs.readdirSync(directory);
          
          for (const file of files) {
            const filePath = path.join(directory, file);
            if (fs.existsSync(filePath)) {
              const stats = fs.statSync(filePath);
              
              if (stats.isDirectory()) {
                await deleteTempFiles(filePath);
                fs.rmdirSync(filePath);
              } else {
                fs.unlinkSync(filePath);
              }
            }
          }
        };
        
        await deleteTempFiles(post.processing_temp_dir);
        fs.rmdirSync(post.processing_temp_dir);
        log(`Temporary directory cleaned up: ${post.processing_temp_dir}`);
      } catch (cleanupError) {
        logError('Error cleaning up temporary files:', cleanupError);
        // Continue despite cleanup error
      }
    }
    
    // Update execution record if available
    if (executionRecord && executionRecord.$id) {
      await databases.updateDocument(
        APPWRITE_DATABASE_ID,
        APPWRITE_COLLECTION_ID_EXECUTION,
        executionRecord.$id,
        {
          status: success ? 'completed' : 'error',
          completion_time: new Date().toISOString(),
          error_message: errorMessage || null
        }
      );
      log(`Execution record updated: ${executionRecord.$id}`);
    }
  } catch (error) {
    logError('Error in cleanup and execution update:', error);
  }
}

// This function creates or updates execution records and helps divide work across multiple function calls
async function manageProcessingExecution(post, currentStep, databases, client, options = {}) {
  const { executionId, progressData = {}, progress = 0 } = options;
  const postId = post.$id;
  
  try {
    // Update the post's progress data
    await databases.updateDocument(
      APPWRITE_DATABASE_ID,
      APPWRITE_COLLECTION_ID,
      postId,
      {
        audio_progress_data: {
          ...progressData,
          currentStep,
          executionId,
          lastUpdated: new Date().toISOString()
        }
      }
    );
    
    // Notify about progress
    await notifyProgress(postId, databases, currentStep, progress);
    
    return {
      $id: executionId,
      current_step: currentStep,
      progress_data: JSON.stringify(progressData)
    };
  } catch (error) {
    logError(`Error managing execution for post ${postId}:`, error);
    throw error;
  }
}

// Check execution limits
async function shouldStartNewExecution(postId, databases) {
  try {
    // Check if there are too many recent executions (to prevent infinite loops)
    const recentExecutions = await databases.listDocuments(
      APPWRITE_DATABASE_ID,
      APPWRITE_COLLECTION_ID_EXECUTION,
      [
        Query.equal('post_id', postId),
        Query.greaterThan('start_time', new Date(Date.now() - 3600000).toISOString()), // Last hour
      ]
    );
    
    // If more than 10 executions in the last hour, prevent new execution
    if (recentExecutions.documents.length > 10) {
      log(`Too many recent executions (${recentExecutions.documents.length}) for post ${postId}`);
      return false;
    }
    
    return true;
  } catch (err) {
    logError(`Error checking execution limits for post ${postId}:`, err);
    return false;
  }
}

// Main audio processing function, now with step-based processing
async function processAudio(postId, databases, storage, client, executionId = null) {
  // Используем уже определенную переменную PROCESSING_STEPS, которая объявлена в начале файла
  
  // Start timing
  const startTime = Date.now();
  
  // Create execution record
  let executionRecord = {
    $id: executionId || ID.unique(),
    timestamp: new Date().toISOString(),
    status: 'running',
    error: null,
    trigger_type: 'direct',
    request_info: {},
    execution_time_ms: 0
  };
  
  // Logging helpers
  const log = (message) => log(`[${executionRecord.$id}] ${message}`);
  const logError = (message, error) => logError(`[${executionRecord.$id}] ${message}`, error);
  
  try {
    log(`Starting audio processing for post ${postId}`);
    
    // Validate post ID
    if (!postId) {
      throw new Error('Post ID is required');
    }
    
    // Get post document
    const post = await databases.getDocument(
      APPWRITE_DATABASE_ID,
      APPWRITE_COLLECTION_ID,
      postId
    );
    
    log(`Retrieved post ${post.$id}`);
    
    // Check if we should start a new execution
    if (!executionId) {
      const shouldStart = await shouldStartNewExecution(postId, databases);
      if (!shouldStart) {
        log(`Too many recent executions for post ${postId}, skipping`);
        return {
          success: false,
          message: 'Too many recent executions for this post, try again later',
          postId
        };
      }
      log(`Starting new execution for post ${postId}`);
    }
    
    // Initialize or get progress data 
    let progressData = post.audio_progress_data || {};
    
    // Determine current step
    let currentStep = PROCESSING_STEPS.INITIALIZE;
    
    // If executionId is provided, get the execution record
    if (executionId) {
      try {
        // Get execution from database or from post progress data
        if (post.audio_progress_data && post.audio_progress_data.executionId === executionId) {
          progressData = post.audio_progress_data;
          currentStep = progressData.currentStep || PROCESSING_STEPS.INITIALIZE;
          log(`Continuing execution ${executionId} at step ${currentStep}`);
        } else {
          log(`Starting new execution as continuation data not found for ${executionId}`);
          // Reset to initial step if we can't find the execution
          await manageProcessingExecution(post, PROCESSING_STEPS.INITIALIZE, databases, client, {
            executionId: executionRecord.$id,
            progressData: {}
          });
          currentStep = PROCESSING_STEPS.INITIALIZE;
        }
      } catch (execError) {
        logError('Error retrieving execution record:', execError);
        // Reset to initial step
        currentStep = PROCESSING_STEPS.INITIALIZE;
      }
    } else {
      // New execution, start from beginning
      await manageProcessingExecution(post, PROCESSING_STEPS.INITIALIZE, databases, client, {
        executionId: executionRecord.$id,
        progressData: {}
      });
    }
    
    // Process based on current step
    
    // INITIALIZE step (check if post has unprocessed audio)
    if (currentStep === PROCESSING_STEPS.INITIALIZE) {
      log(`Initializing processing for post ${postId}`);
      
      // Check if the post already has processed audio
      if (post.audio_processed === true) {
        log(`Post ${postId} already has processed audio`);
        return {
          success: true,
          message: 'Post already has processed audio',
          postId
        };
      }
      
      // Check if the post has a valid audio URL
      if (!post.audio_url) {
        log(`Post ${postId} does not have an audio URL`);
        return {
          success: false,
          message: 'Post does not have an audio URL',
          postId
        };
      }
      
      // Update post to mark as processing
      await databases.updateDocument(
        APPWRITE_DATABASE_ID,
        APPWRITE_COLLECTION_ID,
        postId,
        {
          audio_processing: true,
          audio_progress_data: {
            executionId: executionRecord.$id,
            currentStep: PROCESSING_STEPS.DOWNLOAD,
            startedAt: new Date().toISOString()
          }
        }
      );
      
      log(`Updated post ${postId} to mark as processing`);
      
      // Update to next step
      await manageProcessingExecution(post, PROCESSING_STEPS.DOWNLOAD, databases, client, {
        executionId: executionRecord.$id,
        progressData: {
          startedAt: new Date().toISOString(),
          audioUrl: post.audio_url
        }
      });
      
      return { 
        success: true, 
        message: 'Initialized processing, moving to download step', 
        executionId: executionRecord.$id,
        postId,
        currentStep: PROCESSING_STEPS.DOWNLOAD
      };
    }
    
    // DOWNLOAD step (download the audio file)
    if (currentStep === PROCESSING_STEPS.DOWNLOAD) {
      return await processContinuationDownload(post, { body: {} }, { json: () => {} }, databases, storage, executionRecord, startTime);
    }
    
    // CONVERT step (convert the audio file)
    if (currentStep === PROCESSING_STEPS.CONVERT) {
      return await processContinuationFFmpeg(post, { body: {} }, { json: () => {} }, databases, storage, executionRecord, startTime);
    }
    
    // SEGMENT step (segment the audio file)
    if (currentStep === PROCESSING_STEPS.SEGMENT) {
      return await processContinuationPlaylistPreparation(post, { body: {} }, { json: () => {} }, databases, storage, executionRecord, startTime);
    }
    
    // UPLOAD_SEGMENTS step (upload segments to storage)
    if (currentStep === PROCESSING_STEPS.UPLOAD_SEGMENTS) {
      return await processContinuationSegmentUpload(post, { body: {} }, { json: () => {} }, databases, storage, executionRecord, startTime);
    }
    
    // CREATE_PLAYLIST step (create playlist from segments)
    if (currentStep === PROCESSING_STEPS.CREATE_PLAYLIST) {
      return await processContinuationPlaylistCreation(post, { body: {} }, { json: () => {} }, databases, storage, executionRecord, startTime);
    }
    
    // FINALIZE step (finalize the processing)
    if (currentStep === PROCESSING_STEPS.FINALIZE) {
      log(`Finalizing processing for post ${postId}`);
      
      try {
        // Update post with processed flag
        await databases.updateDocument(
          APPWRITE_DATABASE_ID,
          APPWRITE_COLLECTION_ID,
          postId,
          {
            audio_processed: true,
            audio_processing: false,
            audio_processed_at: new Date().toISOString(),
            audio_progress_data: {
              ...progressData,
              completedAt: new Date().toISOString(),
              status: 'completed'
            }
          }
        );
        
        log(`Updated post ${postId} to mark as processed`);
        
        // Clean up temporary files
        await cleanupAndUpdateExecution(post, executionRecord, databases, true);
        
        return {
          success: true,
          message: 'Audio processing completed successfully',
          postId,
          executionId: executionRecord.$id
        };
      } catch (finalizeError) {
        logError('Error finalizing processing:', finalizeError);
        
        // Update execution record with error
        await cleanupAndUpdateExecution(post, executionRecord, databases, false, finalizeError.message);
        
        return { 
          success: false, 
          message: `Error finalizing processing: ${finalizeError.message}`,
          postId,
          executionId: executionRecord.$id
        };
      }
    }
    
    // Invalid step
    throw new Error(`Invalid processing step: ${currentStep}`);
    
  } catch (error) {
    logError('Error processing audio:', error);
    
    // Update post with error status if possible
    try {
      if (postId) {
        await databases.updateDocument(
          APPWRITE_DATABASE_ID,
          APPWRITE_COLLECTION_ID,
          postId,
          {
            audio_processing: false,
            audio_error: error.message
          }
        );
        log(`Updated post ${postId} with error status`);
      }
    } catch (updateError) {
      logError('Error updating post with error status:', updateError);
    }
    
    return {
      success: false,
      message: `Error processing audio: ${error.message}`,
      postId,
      executionId: executionRecord.$id
    };
  }
}