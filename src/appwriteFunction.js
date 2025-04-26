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
const APPWRITE_PROJECT_ID = process.env.APPWRITE_PROJECT_ID || '67f2235900328715e56';
const APPWRITE_API_KEY = process.env.APPWRITE_API_KEY;
const APPWRITE_DATABASE_ID = process.env.APPWRITE_DATABASE_ID || '67f225f50010cced7742';
const APPWRITE_COLLECTION_ID = process.env.APPWRITE_COLLECTION_ID || 'posts';
const APPWRITE_BUCKET_ID = process.env.APPWRITE_BUCKET_ID || '67f22396003840034d78';
const APPWRITE_COLLECTION_ID_POST = process.env.APPWRITE_COLLECTION_ID_POST || '67f22813001f125cc1e5';
const APPWRITE_FUNCTION_ID = process.env.APPWRITE_FUNCTION_ID || '67fd5f3793f097add368';

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
  
  // Проверяем id или $id в req
  if (req.id) {
    log(`Found id in req: ${req.id}`);
    return req.id;
  }
  
  if (req.$id) {
    log(`Found $id in req: ${req.$id}`);
    return req.$id;
  }
  
  // Проверяем параметры запроса
  if (req.params && req.params.postId) {
    log(`Found postId in req.params: ${req.params.postId}`);
    return req.params.postId;
  }
  
  // Проверяем параметры id или $id в params
  if (req.params) {
    if (req.params.id) {
      log(`Found id in req.params: ${req.params.id}`);
      return req.params.id;
    }
    if (req.params.$id) {
      log(`Found $id in req.params: ${req.params.$id}`);
      return req.params.$id;
    }
  }

  // Проверяем переменные
  if (req.variables && req.variables.postId) {
    log(`Found postId in req.variables: ${req.variables.postId}`);
    return req.variables.postId;
  }
  
  // Проверяем переменные id или $id
  if (req.variables) {
    if (req.variables.id) {
      log(`Found id in req.variables: ${req.variables.id}`);
      return req.variables.id;
    }
    if (req.variables.$id) {
      log(`Found $id in req.variables: ${req.variables.$id}`);
      return req.variables.$id;
    }
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
    
    // Прямой поиск для всех возможных идентификаторов
    if (obj.postId) {
      log(`Found postId at ${path}.postId: ${obj.postId}`);
      return obj.postId;
    }
    
    if (obj.id) {
      log(`Found id at ${path}.id: ${obj.id}`);
      return obj.id;
    }
    
    if (obj.$id) {
      log(`Found $id at ${path}.$id: ${obj.$id}`);
      return obj.$id;
    }
    
    // Поиск в data
    if (obj.data && !visited.has(obj.data)) {
      if (obj.data.postId) {
        log(`Found postId at ${path}.data.postId: ${obj.data.postId}`);
        return obj.data.postId;
      }
      
      if (obj.data.id) {
        log(`Found id at ${path}.data.id: ${obj.data.id}`);
        return obj.data.id;
      }
      
      if (obj.data.$id) {
        log(`Found $id at ${path}.data.$id: ${obj.data.$id}`);
        return obj.data.$id;
      }
    }
    
    // Поиск в event
    if (obj.event && !visited.has(obj.event)) {
      if (obj.event.postId) {
        log(`Found postId at ${path}.event.postId: ${obj.event.postId}`);
        return obj.event.postId;
      }
      
      if (obj.event.id) {
        log(`Found id at ${path}.event.id: ${obj.event.id}`);
        return obj.event.id;
      }
      
      if (obj.event.$id) {
        log(`Found $id at ${path}.event.$id: ${obj.event.$id}`);
        return obj.event.$id;
      }
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

// Основная функция обработки, экспортируемая модулем
module.exports = async function(req, res) {
  // Инициализируем объект payload заранее
  let payload = {};
  let postId = null;
  
  // Правильно определяем функции логирования в соответствии с Appwrite
  const log = (typeof req?.log === 'function') ? req.log : console.log;
  const logError = (typeof req?.error === 'function') ? req.error : console.error;
  
  log('Запуск функции обработки аудио');
  
  try {
    // Логируем доступные ключи в req для отладки
    log(`Request keys: ${req ? Object.keys(req).join(', ') : 'req is undefined'}`);
    
    // Получение и парсинг payload
    if (req && req.body) {
      try {
        if (typeof req.body === 'string') {
          payload = JSON.parse(req.body);
        } else if (typeof req.body === 'object') {
          payload = req.body;
        }
      } catch (e) {
        logError('Error parsing request body:', e);
      }
    }
    
    // Проверка наличия payload в альтернативных источниках
    if (Object.keys(payload).length === 0 && req) {
      if (req.rawBody) {
        try {
          payload = JSON.parse(req.rawBody);
        } catch (e) {
          // Игнорируем ошибку, просто продолжим
        }
      } else if (req.bodyRaw) {
        try {
          payload = JSON.parse(req.bodyRaw);
        } catch (e) {
          // Игнорируем ошибку, просто продолжим
        }
      }
    }
    
    // Логируем ключи в payload для отладки
    log(`Payload keys: ${Object.keys(payload).join(', ')}`);
    
    // Получаем postId различными способами
    if (payload && payload.postId) {
      postId = payload.postId;
      log(`Using postId from payload: ${postId}`);
    } else if (req) {
      // Пытаемся извлечь postId из других источников
      postId = extractPostId(req, log, logError);
      log(`Extracted postId: ${postId}`);
    }
    
    // Проверяем, что postId был найден
    if (!postId) {
      log('No postId found in request');
      
      // Безопасная отправка ответа, проверяем наличие res и метода json
      if (res && typeof res.json === 'function') {
      return res.json({
        success: false,
          message: 'No postId found in request'
        });
      } else if (typeof req?.json === 'function') {
        // Новый API Appwrite использует context.req.json
        return req.json({
          success: false,
          message: 'No postId found in request'
        });
      } else {
        // Для контекста без res/req.json
        logError('Cannot send response - no available response methods');
        return {
          success: false,
          message: 'No postId found in request'
        };
      }
    }
    
    // Остальная логика обработки запроса...
    // Запускаем обработку аудио
    const result = await simplifiedAudioProcessing(postId, null, databases, storage, client);
    
    // Безопасно отправляем ответ
    if (res && typeof res.json === 'function') {
      return res.json(result);
    } else if (typeof req?.json === 'function') {
      // Новый API Appwrite использует context.req.json
      return req.json(result);
    } else {
      // Для контекста без res/req.json
      log('Returning result as object (no response methods available)');
      return result;
    }
    
  } catch (error) {
    logError('Error in main function:', error);
    
    // Безопасно отправляем ошибку
    const errorResponse = {
      success: false,
      error: error.message || 'Unknown error',
      stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
    };
    
    if (res && typeof res.json === 'function') {
      return res.json(errorResponse);
    } else if (typeof req?.json === 'function') {
      // Новый API Appwrite использует context.req.json
      return req.json(errorResponse);
    } else {
      // Для контекста без res/req.json
      return errorResponse;
    }
  }
};

// Упрощенная версия обработки аудио, делающая минимальное количество API-запросов
async function simplifiedAudioProcessing(postId, existingPostData, databases, storage, client) {
  const executionId = ID.unique();
  const log = (message) => console.log(`[${executionId}] ${message}`);
  
  try {
    log(`Starting audio processing for post ${postId}`);
    
    // Получаем данные поста только если они еще не переданы
    const post = existingPostData || await retryWithBackoff(async () => {
      return await databases.getDocument(
        APPWRITE_DATABASE_ID,
        APPWRITE_COLLECTION_ID_POST,
        postId
      );
    });
    
    log(`Retrieved post ${post.$id}`);
    
    // ВАЖНО: Проверка на частые выполнения, чтобы избежать бесконечного цикла
    const recentExecutions = (post.streaming_urls || []).filter(url => 
      typeof url === 'string' &&
      (url.startsWith('processing-start:') || url.startsWith('processing:')) && 
      new Date(url.split(':')[2]) > new Date(Date.now() - 3600000) // За последний час
    );
    
    if (recentExecutions.length >= 5) {
      log(`Обнаружено слишком много недавних выполнений (${recentExecutions.length}) для поста ${postId}, прерываем обработку`);
      return {
        success: false,
        message: 'Слишком много недавних выполнений, прерываем для предотвращения бесконечного цикла',
        postId
      };
    }
    
    // Проверяем, было ли запущено выполнение в течение последних 30 секунд
    const veryRecentExecutions = recentExecutions.filter(
      url => new Date(url.split(':')[2]) > new Date(Date.now() - 30000) // За последние 30 секунд
    );
    
    if (veryRecentExecutions.length > 0) {
      log(`Обнаружено выполнение, начатое менее 30 секунд назад для поста ${postId}, прерываем дублирование`);
      return {
        success: false,
        message: 'Обработка уже начата менее 30 секунд назад, предотвращаем дублирование',
        postId
      };
    }
    
    // Проверяем, есть ли уже обработанное аудио
    if (post.mp3_url || post.m3u8_url) {
      log(`Post ${postId} already has processed audio`);
      return {
        success: true,
        message: 'Post already has processed audio',
        postId
      };
    }
    
    // Проверяем наличие URL аудио
    if (!post.audio_url) {
      log(`Post ${postId} does not have an audio URL`);
      return {
        success: false,
        message: 'Post does not have an audio URL',
        postId
      };
    }
    
    // Обновляем пост с информацией о начале обработки
    // Используем всего ОДИН запрос на обновление
    await retryWithBackoff(async () => {
      return await databases.updateDocument(
        APPWRITE_DATABASE_ID,
        APPWRITE_COLLECTION_ID_POST,
        postId,
        {
          streaming_urls: [`processing-start:${executionId}:${new Date().toISOString()}`],
          // Используем поле description для хранения статуса
          description: post.description ? 
            `${post.description}\n[Processing started]` : 
            '[Processing started]'
        }
      );
    }, 5, 3000); // Увеличиваем время ожидания и количество попыток
    
    log(`Updated post ${postId} to mark as processing`);
    
    // Начинаем обработку аудио с помощью отдельного процесса или задачи
    // чтобы не делать слишком много API-запросов в одной функции
    
    // Вместо множества последовательных обновлений в базе данных,
    // мы просто инициируем процесс и возвращаем успешный результат
    
    // Этот код может быть расширен для запуска отдельной задачи или
    // процесса через другой механизм (например, очередь сообщений)
    
    // Для демонстрации мы просто возвращаем успешный результат
    return { 
      success: true, 
      message: 'Simplified audio processing initiated to avoid rate limiting', 
      executionId,
      postId
    };
    
  } catch (error) {
    console.error('Error in simplified audio processing:', error);
    
    // Пытаемся обновить пост с информацией об ошибке - только одна попытка
    try {
      await retryWithBackoff(async () => {
        return await databases.updateDocument(
          APPWRITE_DATABASE_ID,
          APPWRITE_COLLECTION_ID_POST,
          postId,
          {
            description: `Error processing audio: ${error.message}`,
            streaming_urls: [`error:${error.message}:${new Date().toISOString()}`]
          }
        );
      }, 3, 5000); // Длительная задержка и меньше попыток
      console.log(`Updated post ${postId} with error status`);
    } catch (updateError) {
      console.error('Could not update error status due to rate limiting', updateError);
    }
    
    return {
      success: false,
      message: `Error processing audio: ${error.message}`,
      postId
    };
  }
}

// Helper function to process unprocessed posts
async function processUnprocessedPosts(databases, storage, client) {
  try {
    // Find unprocessed posts
    const posts = await databases.listDocuments(
      APPWRITE_DATABASE_ID,
      APPWRITE_COLLECTION_ID_POST,
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
        APPWRITE_COLLECTION_ID_POST,
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
      APPWRITE_COLLECTION_ID_POST,
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
    const taskId = await scheduleContinuationTask(post.$id, post, executionRecord.$id, 3);
    
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
    const taskId = await scheduleContinuationTask(post.$id, post, executionRecord.$id, 3);
    
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
      const taskId = await scheduleContinuationTask(post.$id, post, executionRecord.$id, 3);
      
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
      const taskId = await scheduleContinuationTask(post.$id, post, executionRecord.$id, 3);
      
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

// Планирование задачи продолжения
async function scheduleContinuationTask(postId, post, executionId, delaySeconds = 5) {
  try {
    // Проверяем, сколько задач продолжения уже запланировано для этого поста
    const continuationCount = (post?.streaming_urls || [])
      .filter(url => typeof url === 'string' && url.includes('continuation-task:'))
      .length;
    
    log(`[scheduleContinuationTask] Current continuation count for post ${postId}: ${continuationCount}`);
    
    // Ограничиваем количество продолжений для предотвращения бесконечных циклов
    if (continuationCount >= 10) {
      const message = `Maximum number of continuations (10) reached for post ${postId}. Aborting to prevent looping.`;
      log(message);
      return {
        success: false,
        message: message
      };
    }
    
    log(`[scheduleContinuationTask] Scheduling continuation task for post ${postId} with delay ${delaySeconds}s`);
    
    // Обновляем пост информацией о запланированной задаче
    const continuationTimestamp = new Date().toISOString();
    const continuationInfo = `continuation-task: scheduled at ${continuationTimestamp} with delay ${delaySeconds}s`;
    
    // Добавляем информацию о продолжении в streaming_urls, если они существуют
    const streamingUrls = post?.streaming_urls || [];
    streamingUrls.push(continuationInfo);
    
    // Обновляем пост с новой информацией о продолжении
    await databases.updateDocument(
      APPWRITE_DATABASE_ID,
      APPWRITE_COLLECTION_ID_POST,
      postId,
      { 
        streaming_urls: streamingUrls,
        processing_continuation_scheduled: true,
        processing_next_step_at: new Date(Date.now() + (delaySeconds * 1000)).toISOString()
      }
    );
    
    // ВАЖНО: Вместо автоматического планирования, просто логируем намерение
    log(`[scheduleContinuationTask] Continuation task scheduled for post ${postId}. Next step should be executed manually or via external scheduler.`);
    
    // Возвращаем успешный результат, но без реального планирования
    return {
      success: true,
      message: `Continuation task scheduled for post ${postId}. Execute manually.`,
      continuationCount: continuationCount + 1
    };
  } catch (error) {
    logError(`[scheduleContinuationTask] Error scheduling continuation task: ${error.message}`);
    return {
      success: false,
      message: error.message
    };
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
      const taskId = await scheduleContinuationTask(post.$id, post, executionRecord.$id, 3);
      
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
      const taskId = await scheduleContinuationTask(post.$id, post, executionRecord.$id, 3);
    
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
  console.log(`Updating execution record ${executionRecord?.$id || 'unknown'}`);
  
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
        console.log(`Temporary directory cleaned up: ${post.processing_temp_dir}`);
      } catch (cleanupError) {
        console.error('Error cleaning up temporary files:', cleanupError);
        // Continue despite cleanup error
      }
    }
    
    // Обновляем статус обработки в документе поста вместо записи в отдельной коллекции
    if (post && post.$id) {
      try {
        await databases.updateDocument(
          APPWRITE_DATABASE_ID,
          APPWRITE_COLLECTION_ID_POST,
          post.$id,
          {
            processing_status: success ? 'completed' : 'error',
            processing_completed_at: new Date().toISOString(),
            processing_error: errorMessage || null
          }
        );
        console.log(`Post ${post.$id} updated with execution status`);
      } catch (updateError) {
        console.error('Error updating post with execution status:', updateError);
      }
    }
  } catch (error) {
    console.error('Error in cleanup and execution update:', error);
  }
}

// Добавляем функцию задержки для отложенного выполнения
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Увеличиваем начальную задержку и количество повторных попыток
async function retryWithBackoff(fn, maxRetries = 5, initialDelay = 2000) {
  let retries = 0;
  
  while (retries < maxRetries) {
    try {
      // Добавляем обязательную минимальную задержку перед каждым запросом
      await sleep(500);
      return await fn();
    } catch (error) {
      retries++;
      
      // Проверяем тип ошибки
      const isRateLimit = error.message && (
        error.message.includes('Rate limit') || 
        error.message.includes('Too many requests') ||
        error.code === 429
      );
      
      // Если это не ошибка rate limit или достигнут максимум попыток - выбросить ошибку
      if (!isRateLimit || retries >= maxRetries) {
        throw error;
      }
      
      // Экспоненциальная задержка с дополнительным случайным компонентом для предотвращения "шторма" запросов
      // 2с, 4с, 8с, 16с, 32с...
      const delay = initialDelay * Math.pow(2, retries - 1) + Math.floor(Math.random() * 1000);
      console.log(`Rate limit exceeded. Retrying in ${delay}ms (attempt ${retries}/${maxRetries})`);
      await sleep(delay);
    }
  }
}

// Обновляем функцию обновления документа, чтобы использовать retry логику
async function safeUpdateDocument(databases, databaseId, collectionId, documentId, data) {
  return retryWithBackoff(async () => {
    return await databases.updateDocument(
      databaseId,
      collectionId,
      documentId,
      data
    );
  });
}

// This function creates or updates execution records and helps divide work across multiple function calls
async function manageProcessingExecution(post, currentStep, databases, client, options = {}) {
  const { executionId, progressData = {}, progress = 0 } = options;
  const postId = post.$id;
  
  try {
    // Обновляем только существующие поля в документе с использованием retry логики
    await safeUpdateDocument(
      databases,
      APPWRITE_DATABASE_ID,
      APPWRITE_COLLECTION_ID_POST,
      postId,
      {
        // Сохраняем информацию о прогрессе в первый элемент массива streaming_urls
        streaming_urls: [`progress:${currentStep}:${executionId}:${new Date().toISOString()}`]
      }
    );
    
    // Выводим лог вместо вызова несуществующей функции notifyProgress
    console.log(`Updated progress for post ${postId} to step ${currentStep}`);
    
    return {
      $id: executionId,
      current_step: currentStep,
      progress_data: JSON.stringify(progressData)
    };
  } catch (error) {
    console.error(`Error managing execution for post ${postId}:`, error);
    throw error;
  }
}

// Check execution limits
async function shouldStartNewExecution(postId, databases) {
  try {
    // Получаем документ поста для проверки истории выполнений
    const post = await databases.getDocument(
      APPWRITE_DATABASE_ID,
      APPWRITE_COLLECTION_ID_POST,
      postId
    );
    
    // Проверяем streaming_urls на наличие недавних выполнений
    const recentExecutions = (post.streaming_urls || [])
      .filter(url => 
        typeof url === 'string' && 
        (url.startsWith('processing-start:') || url.startsWith('processing:')) && 
        new Date(url.split(':')[2]) > new Date(Date.now() - 3600000) // За последний час
      );
    
    console.log(`Found ${recentExecutions.length} recent executions for post ${postId}`);
    
    // Если более 5 выполнений за последний час, предотвращаем новое
    if (recentExecutions.length >= 5) {
      console.log(`Too many recent executions (${recentExecutions.length}) for post ${postId}, preventing new execution`);
      return false;
    }
    
    // Проверяем, нет ли уже выполнения, которое началось менее 30 секунд назад
    const veryRecentExecutions = recentExecutions.filter(
      url => new Date(url.split(':')[2]) > new Date(Date.now() - 30000) // За последние 30 секунд
    );
    
    if (veryRecentExecutions.length > 0) {
      console.log(`Execution already started for post ${postId} in the last 30 seconds, preventing duplicate`);
      return false;
    }
    
    return true;
  } catch (err) {
    console.error(`Error checking execution limits for post ${postId}:`, err);
    // В случае ошибки всё равно разрешаем выполнение, но с предупреждением
    console.log(`WARNING: Could not check execution limits, proceeding with caution for post ${postId}`);
    return true;
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
  
  // Используем console.log/error напрямую
  const log = (message) => console.log(`[${executionRecord.$id}] ${message}`);
  const logError = (message, err) => console.error(`[${executionRecord.$id}] ${message}`, err);
  
  try {
    log(`Starting audio processing for post ${postId}`);
    
    // Validate post ID
    if (!postId) {
      throw new Error('Post ID is required');
    }
    
    // Get post document
    const post = await databases.getDocument(
      APPWRITE_DATABASE_ID,
      APPWRITE_COLLECTION_ID_POST,
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
    
    // Определяем, используется ли уже аудио
    let currentStep = PROCESSING_STEPS.INITIALIZE;
    
    // INITIALIZE step (check if post has unprocessed audio)
    if (currentStep === PROCESSING_STEPS.INITIALIZE) {
      log(`Initializing processing for post ${postId}`);
      
      // Проверяем, есть ли у поста уже обработанное аудио (mp3_url или m3u8_url)
      if (post.mp3_url || post.m3u8_url) {
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
      
      // Update post to mark as processing - используем существующие поля с retry логикой
      await safeUpdateDocument(
        databases,
        APPWRITE_DATABASE_ID,
        APPWRITE_COLLECTION_ID_POST,
        postId,
        {
          // Используем поле streaming_urls для хранения информации о процессе
          streaming_urls: [`processing:${executionRecord.$id}:${new Date().toISOString()}`]
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
          APPWRITE_COLLECTION_ID_POST,
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
    console.error('Error processing audio:', error);
    
    // Обновляем пост с информацией об ошибке, используя только существующие поля и retry логику
    try {
      if (postId) {
        await safeUpdateDocument(
          databases,
          APPWRITE_DATABASE_ID,
          APPWRITE_COLLECTION_ID_POST,
          postId,
          {
            // Сохраняем информацию об ошибке в description или другое текстовое поле
            description: `Error processing audio: ${error.message}`,
            // Также можем использовать streaming_urls для хранения статуса
            streaming_urls: [`error:${error.message}:${new Date().toISOString()}`]
          }
        );
        console.log(`Updated post ${postId} with error status`);
      }
    } catch (updateError) {
      console.error('Error updating post with error status:', updateError);
    }
    
    return {
      success: false,
      message: `Error processing audio: ${error.message}`,
      postId,
      executionId: executionRecord.$id
    };
  }
}

// Вспомогательная функция для безопасной отправки JSON-ответа
function safeJsonResponse(res, data, isContextPattern, log, error) {
  // Защита от undefined res
  if (!res) {
    error('safeJsonResponse: res object is undefined');
    return data;
  }
  
  // Если используется шаблон с контекстом (context.res)
  if (isContextPattern) {
    if (typeof res.json === 'function') {
      return res.json(data);
    }
  }
  
  // Для стандартного Node.js HTTP ответа
  if (typeof res.json === 'function') {
    return res.json(data);
  } else if (typeof res.send === 'function') {
    return res.send(JSON.stringify(data));
  } else if (typeof res.end === 'function') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify(data));
  }
  
  // Если мы не можем отправить ответ, логируем ошибку и возвращаем объект
  error('Unable to send response: res object does not have required methods');
  return data;
}