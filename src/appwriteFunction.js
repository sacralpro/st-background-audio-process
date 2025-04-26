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

// Добавляем кеш для отслеживания обрабатываемых постов и предотвращения параллельной обработки
const processingPosts = new Map();
// Добавляем счетчик выполнений, чтобы отслеживать, сколько раз пост был обработан
const postExecutionCounts = new Map();

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
  // Создаем уникальный ID выполнения для логирования
  const executionId = ID.unique();
  const startTime = Date.now();
  
  // Инициализируем объект payload заранее
  let payload = {};
  let postId = null;
  let context = null;
  let source = 'unknown';
  let isTest = false;
  let isContinuation = false;      // НОВОЕ: флаг продолжения обработки
  let continuationExecutionId = null; // НОВОЕ: ID первоначального выполнения
  
  // Проверяем, может быть у нас новый формат контекста Appwrite
  if (req && req.req && req.res) {
    context = req;
    req = context.req || {};
    res = context.res || {};
  }
  
  // Правильно определяем функции логирования в соответствии с Appwrite
  const log = (context?.log || req?.log || console.log).bind(console);
  const logError = (context?.error || req?.error || console.error).bind(console);
  
  log(`[${executionId}] Запуск функции обработки аудио`);
  
  try {
    // Логируем доступные ключи в req для отладки
    log(`[${executionId}] Request keys: ${req ? Object.keys(req).join(', ') : 'req is undefined'}`);
    
    // Получение и парсинг payload
    if (req && req.body) {
      try {
        if (typeof req.body === 'string') {
          payload = JSON.parse(req.body);
        } else if (typeof req.body === 'object') {
          payload = req.body;
        }
      } catch (e) {
        logError(`[${executionId}] Error parsing request body:`, e);
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
    log(`[${executionId}] Payload keys: ${Object.keys(payload).join(', ')}`);
    
    // НОВОЕ: Проверяем, является ли запрос продолжением обработки
    if (payload.continuation === true) {
      isContinuation = true;
      log(`[${executionId}] Continuation request detected`);
      
      // Если указан ID выполнения, используем его для логирования
      if (payload.execution_id) {
        continuationExecutionId = payload.execution_id;
        log(`[${executionId}] Continuing execution ${continuationExecutionId}`);
      }
    }
    
    // Проверяем источник запроса
    if (payload.source) {
      source = payload.source;
      log(`[${executionId}] Request source from payload: ${source}`);
    }
    
    // Проверяем, является ли запрос тестовым
    if (payload.isTest === true || payload.test === true) {
      isTest = true;
      log(`[${executionId}] Test request detected`);
    }
    
    // Проверяем заголовки для определения источника
    if (req.headers) {
      const srcHeader = req.headers['x-source'] || req.headers['X-Source'];
      if (srcHeader) {
        source = srcHeader;
        log(`[${executionId}] Request source from header: ${source}`);
      }
      
      // Проверяем, является ли запрос тестовым по заголовку
      const testHeader = req.headers['x-test'] || req.headers['X-Test'];
      if (testHeader === 'true') {
        isTest = true;
        log(`[${executionId}] Test request detected from header`);
      }
    }
    
    // Получаем postId различными способами
    if (payload && payload.postId) {
      postId = payload.postId;
      log(`[${executionId}] Using postId from payload: ${postId}`);
    } else if (payload && payload.id) {
      postId = payload.id;
      log(`[${executionId}] Using id from payload as postId: ${postId}`);
    } else if (req) {
      // Пытаемся извлечь postId из других источников
      postId = extractPostId(req, 
        (msg) => log(`[${executionId}] ${msg}`), 
        (msg, err) => logError(`[${executionId}] ${msg}`, err)
      );
      log(`[${executionId}] Extracted postId: ${postId}`);
    }
    
    // Проверяем, что postId был найден
    if (!postId) {
      log(`[${executionId}] No postId found in request`);
      
      // Формируем ответ
      const errorResponseNoPostId = {
        success: false,
        message: 'No postId found in request',
        executionId,
        executionTime: `${Date.now() - startTime}ms`
      };
      
      // Безопасная отправка ответа, проверяем наличие методов ответа
      return safeResponse(res, errorResponseNoPostId, context, log, logError);
    }
    
    // НОВОЕ: Если это запрос на продолжение обработки, пропускаем некоторые проверки
    if (isContinuation) {
      log(`[${executionId}] Skipping processing lock checks for continuation request`);
      
      try {
        // Получаем документ поста для определения текущего шага
        const post = await databases.getDocument(
          APPWRITE_DATABASE_ID,
          APPWRITE_COLLECTION_ID_POST,
          postId
        );
        
        // Определяем текущий шаг обработки на основе данных в document
        let currentStep = null;
        
        // Ищем информацию о текущем шаге в streaming_urls
        const progressEntries = (post.streaming_urls || []).filter(url => 
          typeof url === 'string' && url.startsWith('progress:')
        );
        
        if (progressEntries.length > 0) {
          // Берем последнюю запись о прогрессе
          const latestProgress = progressEntries[0];
          const progressParts = latestProgress.split(':');
          
          if (progressParts.length > 1) {
            currentStep = progressParts[1];
            log(`[${executionId}] Found current step in streaming_urls: ${currentStep}`);
          }
        }
        
        if (!currentStep && post.processing_status === 'processing') {
          // Если шаг не найден, но статус processing, продолжаем с загрузки
          currentStep = PROCESSING_STEPS.DOWNLOAD;
          log(`[${executionId}] No step found, but status is processing. Continuing with download step`);
        }
        
        // Выполняем соответствующий шаг обработки
        let result;
        if (currentStep === PROCESSING_STEPS.DOWNLOAD) {
          result = await processContinuationDownload(post, req, res, databases, storage, { $id: continuationExecutionId || executionId }, startTime);
        } else if (currentStep === PROCESSING_STEPS.CONVERT) {
          result = await processContinuationFFmpeg(post, req, res, databases, storage, { $id: continuationExecutionId || executionId }, startTime);
        } else if (currentStep === PROCESSING_STEPS.SEGMENT) {
          result = await processContinuationPlaylistPreparation(post, req, res, databases, storage, { $id: continuationExecutionId || executionId }, startTime);
        } else if (currentStep === PROCESSING_STEPS.UPLOAD_SEGMENTS) {
          result = await processContinuationSegmentUpload(post, req, res, databases, storage, { $id: continuationExecutionId || executionId }, startTime);
        } else if (currentStep === PROCESSING_STEPS.CREATE_PLAYLIST) {
          result = await processContinuationPlaylistCreation(post, req, res, databases, storage, { $id: continuationExecutionId || executionId }, startTime);
        } else {
          // Если не можем определить шаг, начинаем с первого шага
          log(`[${executionId}] Could not determine current processing step, restarting from initialize`);
          result = await processAudio(postId, databases, storage, client, continuationExecutionId || executionId);
        }
        
        // Отправляем результат
        return safeResponse(res, {
          ...result,
          isContinuation: true,
          executionId: continuationExecutionId || executionId,
          executionTime: `${Date.now() - startTime}ms`
        }, context, log, logError);
        
      } catch (continuationError) {
        logError(`[${executionId}] Error processing continuation:`, continuationError);
        
        return safeResponse(res, {
          success: false,
          message: `Error processing continuation: ${continuationError.message}`,
          postId,
          executionId,
          isContinuation: true,
          executionTime: `${Date.now() - startTime}ms`
        }, context, log, logError);
      }
    }
    
    // НОВОЕ: Проверка выполняется только если это не запрос на продолжение
    
    // Если функция уже выполняется для этого поста, избегаем запуска еще одного выполнения
    if (processingPosts.has(postId)) {
      const processingStartTime = processingPosts.get(postId);
      const now = Date.now();
      const timeSinceStart = now - processingStartTime;
      
      log(`[${executionId}] Post ${postId} is already being processed (started ${timeSinceStart}ms ago)`);
      
      // Если пост в процессе обработки менее 10 секунд, не запускаем новую обработку
      if (timeSinceStart < 10000) {
        log(`[${executionId}] Preventing duplicate execution as processing started ${timeSinceStart}ms ago`);
        
        return safeResponse(res, {
          success: false,
          message: `Audio processing is already in progress (started ${Math.floor(timeSinceStart/1000)}s ago)`,
          postId,
          executionId,
          alreadyProcessing: true,
          executionTime: `${Date.now() - startTime}ms`
        }, context, log, logError);
      }
      
      // Если процесс идет более 10 секунд, мы можем сбросить блокировку, возможно предыдущий процесс завис
      log(`[${executionId}] Previous processing has been running for ${timeSinceStart}ms, resetting lock`);
    }
    
    // НОВОЕ: Проверяем статус обработки в базе данных перед запуском
    try {
      log(`[${executionId}] Checking post processing status in database`);
      const post = await databases.getDocument(
        APPWRITE_DATABASE_ID,
        APPWRITE_COLLECTION_ID_POST,
        postId
      );
      
      // Проверяем processing_status
      if (post.processing_status === 'processing') {
        // Проверяем, когда последний раз был обновлен пост
        const updatedAt = new Date(post.$updatedAt);
        const now = new Date();
        const timeSinceUpdate = now - updatedAt;
        
        // Если обновление было менее 5 минут назад, считаем что обработка все еще идет
        if (timeSinceUpdate < 5 * 60 * 1000) { // 5 минут в миллисекундах
          log(`[${executionId}] Database indicates post ${postId} is being processed (last update ${Math.floor(timeSinceUpdate/1000)}s ago)`);
          
          // Не запускаем новую обработку, если статус 'processing'
          return safeResponse(res, {
            success: false,
            message: `Post is currently being processed (status: 'processing', last update ${Math.floor(timeSinceUpdate/1000)}s ago)`,
            postId,
            executionId,
            alreadyProcessing: true,
            executionTime: `${Date.now() - startTime}ms`,
            dbCheck: true
          }, context, log, logError);
        } else {
          log(`[${executionId}] Found stale processing status from ${Math.floor(timeSinceUpdate/1000)}s ago, resetting to 'pending'`);
          
          // Если с последнего обновления прошло больше 5 минут, сбрасываем статус на 'pending'
          try {
            await databases.updateDocument(
              APPWRITE_DATABASE_ID,
              APPWRITE_COLLECTION_ID_POST,
              postId,
              {
                processing_status: 'pending'
              }
            );
            log(`[${executionId}] Reset processing_status to 'pending' for post ${postId}`);
          } catch (resetError) {
            logError(`[${executionId}] Error resetting processing_status:`, resetError);
          }
        }
      }
      
      // Если статус 'completed', проверяем наличие обработанных файлов
      if (post.processing_status === 'completed') {
        log(`[${executionId}] Post ${postId} has processing_status 'completed'`);
        
        // Проверяем, чтобы mp3_url или m3u8_url были также установлены
        if (post.mp3_url || post.m3u8_url) {
          log(`[${executionId}] Post ${postId} already has processed audio: mp3_url=${!!post.mp3_url}, m3u8_url=${!!post.m3u8_url}`);
          
          return safeResponse(res, {
            success: true,
            message: 'Post already has processed audio',
            postId,
            executionId,
            mp3_url: post.mp3_url,
            m3u8_url: post.m3u8_url,
            alreadyProcessed: true,
            executionTime: `${Date.now() - startTime}ms`
          }, context, log, logError);
        } else {
          // Если статус 'completed', но ссылок нет, сбрасываем статус
          log(`[${executionId}] Post has status 'completed' but missing audio URLs, resetting to 'pending'`);
          try {
            await databases.updateDocument(
              APPWRITE_DATABASE_ID,
              APPWRITE_COLLECTION_ID_POST,
              postId,
              {
                processing_status: 'pending'
              }
            );
            log(`[${executionId}] Reset processing_status to 'pending' for post ${postId} due to missing audio URLs`);
          } catch (resetError) {
            logError(`[${executionId}] Error resetting processing_status:`, resetError);
          }
        }
      }
      
      // Проверяем поле streaming_urls на наличие свежих записей о процессе обработки
      if (Array.isArray(post.streaming_urls) && post.streaming_urls.length > 0) {
        // Фильтруем записи о начале обработки
        const processingEntries = post.streaming_urls.filter(url => {
          if (typeof url !== 'string') return false;
          return url.startsWith('processing-start:');
        });
        
        if (processingEntries.length > 0) {
          // Проверяем самую свежую запись (они отсортированы в порядке добавления)
          const latestEntry = processingEntries[0]; // самая свежая запись находится в начале массива
          
          // Извлекаем время начала обработки из записи (формат: processing-start:executionId:timestamp:...)
          const parts = latestEntry.split(':');
          if (parts.length >= 3) {
            try {
              const processingStartTime = new Date(parts[2]);
              const now = new Date();
              const timeSinceStart = now - processingStartTime;
              
              // Логируем для информации, но не блокируем, если status не 'processing'
              log(`[${executionId}] Found processing record from ${Math.floor(timeSinceStart/1000)}s ago (for info only)`);
            } catch (e) {
              logError(`[${executionId}] Error parsing processing start time:`, e);
            }
          }
        }
      }
      
    } catch (dbError) {
      logError(`[${executionId}] Error checking post in database:`, dbError);
      // Продолжаем выполнение, даже если проверка базы данных не удалась
    }
    
    // Логируем количество запусков для этого поста
    const currentCount = postExecutionCounts.get(postId) || 0;
    
    // НОВОЕ: Проверяем, не превышено ли максимальное количество выполнений (5 раз)
    if (currentCount >= 5) {
      log(`[${executionId}] Maximum number of executions (5) reached for post ${postId}. Blocking further attempts.`);
      
      return safeResponse(res, {
        success: false,
        message: `Post has reached the maximum number of processing attempts (5).`,
        postId,
        executionId,
        maxExecutionsReached: true,
        executionCount: currentCount,
        executionTime: `${Date.now() - startTime}ms`
      }, context, log, logError);
    }
    
    postExecutionCounts.set(postId, currentCount + 1);
    log(`[${executionId}] Starting execution #${currentCount + 1} for post ${postId}`);
    
    // Устанавливаем блокировку для обработки этого поста
    processingPosts.set(postId, Date.now());
    
    // Запускаем обработку аудио с указанием источника
    log(`[${executionId}] Initializing audio processing for post ${postId} from source: ${source}`);
    const result = await simplifiedAudioProcessing(postId, null, databases, storage, client, {
      source,
      isTest
    });
    
    // Логируем результат
    log(`[${executionId}] Processing completed with result: ${JSON.stringify(result)}`);
    
    // Добавляем информацию о выполнении
    result.executionId = executionId;
    result.executionTime = `${Date.now() - startTime}ms`;
    
    // Безопасно отправляем ответ
    return safeResponse(res, result, context, log, logError);
    
  } catch (error) {
    logError(`[${executionId}] Error in main function:`, error);
    
    // Если у нас есть postId, снимаем блокировку
    if (postId) {
      processingPosts.delete(postId);
      log(`[${executionId}] Removed processing lock for post ${postId} due to error`);
    }
    
    // Безопасно отправляем ошибку
    const errorResponse = {
      success: false,
      error: error.message || 'Unknown error',
      postId,
      executionId,
      executionTime: `${Date.now() - startTime}ms`
    };
    
    if (process.env.NODE_ENV === 'development') {
      errorResponse.stack = error.stack;
    }
    
    return safeResponse(res, errorResponse, context, log, logError);
  }
};

// Улучшенная функция для безопасной отправки ответа, поддерживающая разные API
function safeResponse(res, data, context, log, logError) {
  // Проверяем наличие объекта ответа и методов
  try {
    // Первый способ: обычный Express-подобный res.json()
    if (res && typeof res.json === 'function') {
      log('Sending response using res.json()');
      return res.json(data);
    } 
    
    // Второй способ: Новый API Appwrite V2+
    else if (context && typeof context.send === 'function') {
      log('Sending response using context.send()');
      return context.send(data);
    }
    
    // Третий способ: Вариант с context.res
    else if (context && context.res && typeof context.res.json === 'function') {
      log('Sending response using context.res.json()');
      return context.res.json(data);
    }
    
    // Четвертый способ: req.json() (в некоторых реализациях Appwrite)
    else if (context && context.req && typeof context.req.json === 'function') {
      log('Sending response using context.req.json()');
      return context.req.json(data);
    }
    
    // Пятый способ: res.send()
    else if (res && typeof res.send === 'function') {
      log('Sending response using res.send()');
      return res.send(JSON.stringify(data));
    }
    
    // Шестой способ: Node.js HTTP response
    else if (res && typeof res.end === 'function') {
      log('Sending response using res.end()');
      if (typeof res.setHeader === 'function') {
        res.setHeader('Content-Type', 'application/json');
      } else if (typeof res.writeHead === 'function') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
      }
      return res.end(JSON.stringify(data));
    }
    
    // Нет доступных методов отправки ответа
    logError('No available response methods found. Returning data object directly.');
    return data;
  } catch (responseError) {
    logError('Error sending response:', responseError);
    return data;
  }
}

/**
 * Упрощенная версия функции обработки аудио для одного поста
 * @param {string} postId - ID поста для обработки
 * @param {Object|null} providedPost - Объект поста (если уже получен)
 * @param {Object} databases - Объект для работы с базами данных
 * @param {Object} storage - Объект для работы с хранилищем
 * @param {Object} client - Клиент Appwrite
 * @param {Object} options - Дополнительные опции
 */
async function simplifiedAudioProcessing(postId, providedPost = null, databases, storage, client, options = {}) {
  // Уникальный ID выполнения для логирования
  const executionId = ID.unique();
  const startTime = Date.now();
  
  // Дополнительные опции запуска
  const { source = 'unknown', isTest = false } = options;

  // Очищаем блокировку по завершении через 10 секунд (механизм безопасности)
  const clearLockTimeout = setTimeout(() => {
    if (processingPosts.has(postId)) {
      processingPosts.delete(postId);
      console.log(`[${executionId}] Cleared processing lock for post ${postId} (auto-timeout)`);
    }
  }, 10000);

  try {
    console.log(`[${executionId}] Starting simplified audio processing for post ${postId} from source: ${source}`);
    
    // Проверка источника запроса - разрешаем Sacral Track, тестовые запуски и прямые запросы
    const allowedSources = ['sacral_track', 'appwrite_webhook', 'direct_test', 'unknown', 'appwrite', 'function'];
    
    // Временно отключаем проверку источника, если isTest не явно указан как true
    // Это позволит любым запросам проходить, пока мы тестируем
    if (process.env.DISABLE_SOURCE_CHECK === 'true' || process.env.NODE_ENV === 'development') {
      console.log(`[${executionId}] Source check disabled in development mode or by env var`);
    }
    // Проверка только если включена и не тестовый режим
    else if (!isTest && !allowedSources.includes(source)) {
      console.log(`[${executionId}] Rejected processing request from unauthorized source: ${source}`);
      processingPosts.delete(postId);
      clearTimeout(clearLockTimeout);
      return {
        success: false,
        message: 'Processing request rejected: unauthorized source',
        postId,
        executionId,
        source
      };
    }
    
    // Используем предоставленный пост или загружаем его из базы данных
    let post = providedPost;
    
    if (!post) {
      console.log(`[${executionId}] Retrieving post ${postId} from database`);
      post = await databases.getDocument(
        APPWRITE_DATABASE_ID,
        APPWRITE_COLLECTION_ID_POST,
        postId
      );
      console.log(`[${executionId}] Successfully retrieved post ${post.$id}`);
    }
    
    // Проверка наличия URL аудио
    if (!post.audio_url) {
      console.log(`[${executionId}] Post ${postId} does not have an audio URL`);
      processingPosts.delete(postId);
      clearTimeout(clearLockTimeout);
      return {
        success: false,
        message: 'Post does not have an audio URL to process',
        postId,
        executionId
      };
    }
    
    // Проверка наличия уже обработанного аудио
    if (post.mp3_url || post.m3u8_url) {
      console.log(`[${executionId}] Post ${postId} already has processed audio: mp3_url=${!!post.mp3_url}, m3u8_url=${!!post.m3u8_url}`);
      processingPosts.delete(postId);
      clearTimeout(clearLockTimeout);
      
      // НОВОЕ: Убедимся, что processing_status отмечен как 'completed'
      try {
        await databases.updateDocument(
          APPWRITE_DATABASE_ID,
          APPWRITE_COLLECTION_ID_POST,
          postId,
          {
            processing_status: 'completed'
          }
        );
        console.log(`[${executionId}] Updated processing_status to 'completed' for already processed post`);
      } catch (e) {
        console.error(`[${executionId}] Error updating processing_status for already processed post:`, e);
      }
      
      return {
        success: true,
        message: 'Post already has processed audio',
        postId,
        executionId,
        mp3_url: post.mp3_url,
        m3u8_url: post.m3u8_url,
        alreadyProcessed: true
      };
    }
    
    // Логируем существующие streaming_urls для диагностики
    // Только для информации, не используем для ограничений
    try {
      const streamingUrls = post.streaming_urls || [];
      console.log(`[${executionId}] Current streaming_urls: ${JSON.stringify(streamingUrls)}`);
      
      // Фильтруем только записи о процессе обработки для логирования
      const processingEntries = streamingUrls.filter(url => {
        if (typeof url !== 'string') {
          return false;
        }
        return url.startsWith('processing-start:') || url.startsWith('processing:');
      });
      
      console.log(`[${executionId}] Found ${processingEntries.length} previous processing entries (for info only)`);
    } catch (e) {
      console.error(`[${executionId}] Error while checking previous executions:`, e);
      // Не прекращаем выполнение в случае ошибки проверки
    }
    
    // Обновляем пост с информацией о начале обработки
    try {
      const newStreamingUrl = `processing-start:${executionId}:${new Date().toISOString()}:source=${source}`;
      console.log(`[${executionId}] Updating post with processing start info: ${newStreamingUrl}`);
      
      // Тип streaming_urls должен быть массивом
      let updatedStreamingUrls = [];
      
      if (Array.isArray(post.streaming_urls)) {
        // Оставляем только последние 20 записей для предотвращения переполнения
        updatedStreamingUrls = [newStreamingUrl, ...post.streaming_urls.slice(0, 19)];
      } else {
        updatedStreamingUrls = [newStreamingUrl];
      }
      
      // ИЗМЕНЕНО: Больше не обновляем поле description, используем только streaming_urls
      const updateData = {
        streaming_urls: updatedStreamingUrls,
        processing_status: 'processing'
      };
      
      await retryWithBackoff(async () => {
        return await databases.updateDocument(
          APPWRITE_DATABASE_ID,
          APPWRITE_COLLECTION_ID_POST,
          postId,
          updateData
        );
      }, 5, 3000);
      
      console.log(`[${executionId}] Successfully updated post ${postId} to mark as processing`);
      
    } catch (error) {
      console.error(`[${executionId}] Error updating post with processing start info:`, error);
      // Не прекращаем выполнение в случае ошибки обновления
    }
    
    // Выполняем обработку аудио в отдельном процессе
    console.log(`[${executionId}] Starting actual audio processing for post ${postId}`);
    
    // Здесь выполняется фактическая обработка аудио
    // ИСПРАВЛЕНО: Передаем параметры в правильном порядке
    const result = await processAudio(postId, databases, storage, client, executionId);
    
    // Снимаем блокировку после завершения
    processingPosts.delete(postId);
    clearTimeout(clearLockTimeout);
    
    console.log(`[${executionId}] Audio processing completed for post ${postId} with result:`, result);
    
    // НОВОЕ: Обновляем статус обработки на 'completed' после успешного завершения
    try {
      await databases.updateDocument(
        APPWRITE_DATABASE_ID,
        APPWRITE_COLLECTION_ID_POST,
        postId,
        {
          processing_status: result.success ? 'completed' : 'failed'
        }
      );
      console.log(`[${executionId}] Updated processing_status to '${result.success ? 'completed' : 'failed'}'`);
    } catch (e) {
      console.error(`[${executionId}] Error updating processing_status after completion:`, e);
    }
    
    return result;
  } catch (error) {
    console.error(`[${executionId}] Error in simplifiedAudioProcessing:`, error);
    
    // Снимаем блокировку в случае ошибки
    processingPosts.delete(postId);
    clearTimeout(clearLockTimeout);
    
    // НОВОЕ: Обновляем статус обработки на 'failed' в случае ошибки
    try {
      await databases.updateDocument(
        APPWRITE_DATABASE_ID,
        APPWRITE_COLLECTION_ID_POST,
        postId,
        {
          processing_status: 'failed',
          streaming_urls: Array.isArray(post?.streaming_urls) ? 
            [`processing-error:${executionId}:${new Date().toISOString()}:${error.message}`, ...post.streaming_urls.slice(0, 19)] : 
            [`processing-error:${executionId}:${new Date().toISOString()}:${error.message}`]
        }
      );
      console.log(`[${executionId}] Updated processing_status to 'failed' due to error`);
    } catch (e) {
      console.error(`[${executionId}] Error updating processing_status after error:`, e);
    }
    
    return {
      success: false,
      error: error.message || 'Unknown error',
      postId,
      executionId
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
    // Сначала проверяем, обработан ли уже пост (есть ли у него mp3_url)
    try {
      const post = await databases.getDocument(
        APPWRITE_DATABASE_ID,
        APPWRITE_COLLECTION_ID_POST,
        postId
      );
      
      // Если mp3_url уже существует, прекращаем обработку
      if (post.mp3_url) {
        log(`Post ${postId} already has mp3_url: ${post.mp3_url}. Skipping UI progress update.`);
        return true;
      }
    } catch (fetchError) {
      logError(`Error fetching post to check mp3_url: ${fetchError.message}`);
      // Продолжаем обновлять прогресс в случае ошибки проверки
    }
    
    const progressPercentage = getProgressPercentage(step, progress);
    
    // Подготовка данных для обновления - УДАЛЕНЫ лишние поля
    const updateData = {
      audio_processing_progress: progressPercentage,
      // Оставляем только основные поля
      processing_status: status
    };
    
    await databases.updateDocument(
      APPWRITE_DATABASE_ID,
      APPWRITE_COLLECTION_ID_POST,
      postId,
      updateData
    );
    
    log(`Updated UI progress for post ${postId}: ${step} - ${progressPercentage}%, status: ${status}`);
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
    
    // НОВОЕ: Реально планируем следующий шаг с помощью Appwrite Client SDK
    try {
      // Создаем таймаут, который выполнится через указанное количество секунд
      setTimeout(async () => {
        try {
          log(`[scheduleContinuationTask] Executing continuation task for post ${postId}`);
          
          // Создаем вызов к API для продолжения обработки
          const endpoint = `${process.env.APPWRITE_FUNCTION_ENDPOINT}/functions/${process.env.APPWRITE_FUNCTION_ID}/executions`;
          
          // Подготавливаем данные для выполнения
          const payload = {
            id: postId,
            execution_id: executionId,
            continuation: true
          };
          
          // Если client доступен и есть метод call
          if (client && client.functions && typeof client.functions.createExecution === 'function') {
            // Вызываем функцию через клиент Appwrite
            const execution = await client.functions.createExecution(
              process.env.APPWRITE_FUNCTION_ID,
              JSON.stringify(payload),
              false, // isAsync
              '', // path
              'POST', // method
              {} // headers
            );
            
            log(`[scheduleContinuationTask] Successfully executed continuation, execution ID: ${execution.$id}`);
          } else {
            // Если клиента нет, используем вызов по HTTP
            const response = await fetch(endpoint, {
              method: 'POST',
              headers: {
                'Content-Type': 'application/json',
                'X-Appwrite-Key': process.env.APPWRITE_API_KEY
              },
              body: JSON.stringify({
                data: JSON.stringify(payload),
                async: false
              })
            });
            
            const result = await response.json();
            log(`[scheduleContinuationTask] Continuation task executed via HTTP, result: ${JSON.stringify(result)}`);
          }
        } catch (execError) {
          logError(`[scheduleContinuationTask] Error during continuation execution: ${execError.message}`, execError);
        }
      }, delaySeconds * 1000);
      
      log(`[scheduleContinuationTask] Continuation task scheduled for post ${postId} to execute in ${delaySeconds} seconds`);
    } catch (schedulingError) {
      logError(`[scheduleContinuationTask] Error scheduling actual continuation: ${schedulingError.message}`);
    }
    
    // Возвращаем успешный результат
    return {
      success: true,
      message: `Continuation task scheduled for post ${postId} to execute in ${delaySeconds} seconds`,
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
          processing_status: 'failed', // Изменено с 'error' на 'failed'
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
          processing_status: 'failed', // Изменено с 'error' на 'failed'
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
        const updateData = {
          processing_completed_at: new Date().toISOString(),
          // НОВОЕ: используем общий формат статуса
          processing_status: success ? 'completed' : 'failed',
          // Добавляем запись в streaming_urls для логирования
          streaming_urls: success 
            ? [`processing-complete:${executionRecord?.$id || 'unknown'}:${new Date().toISOString()}`]
            : [`processing-error:${executionRecord?.$id || 'unknown'}:${new Date().toISOString()}:${errorMessage || 'Unknown error'}`]
        };
        
        // Добавляем сообщение об ошибке только если она есть
        if (!success && errorMessage) {
          updateData.processing_error = errorMessage;
        }
        
        await retryWithBackoff(async () => {
          return await databases.updateDocument(
            APPWRITE_DATABASE_ID,
            APPWRITE_COLLECTION_ID_POST,
            post.$id,
            updateData
          );
        }, 5, 3000);
        
        console.log(`Post ${post.$id} updated with execution status: ${success ? 'completed' : 'failed'}`);
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
        streaming_urls: [`progress:${currentStep}:${executionId}:${new Date().toISOString()}`],
        // НОВОЕ: Обновляем статус обработки
        processing_status: 'processing'
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
    
    // НОВАЯ ПРОВЕРКА: Если у поста уже есть mp3_url или m3u8_url, прекращаем обработку
    if (post.mp3_url || post.m3u8_url) {
      log(`Post ${postId} already has processed audio: mp3_url=${!!post.mp3_url}, m3u8_url=${!!post.m3u8_url}. Skipping processing.`);
      
      // Устанавливаем статус completed, если он еще не установлен
      if (post.processing_status !== 'completed') {
        try {
          await databases.updateDocument(
            APPWRITE_DATABASE_ID,
            APPWRITE_COLLECTION_ID_POST,
            postId,
            {
              processing_status: 'completed'
            }
          );
          log(`Updated processing_status to 'completed' for already processed post`);
        } catch (e) {
          logError(`Error updating processing_status for already processed post:`, e);
        }
      }
      
      return {
        success: true,
        message: 'Post already has processed audio',
        postId,
        executionId: executionRecord.$id,
        alreadyProcessed: true,
        mp3_url: post.mp3_url,
        m3u8_url: post.m3u8_url
      };
    }
    
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
          streaming_urls: [`processing:${executionRecord.$id}:${new Date().toISOString()}`],
          // НОВОЕ: Добавляем поле processing_status
          processing_status: 'processing'
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
            // ИЗМЕНЕНО: Больше не используем description для логирования ошибок
            processing_status: 'failed', // Устанавливаем статус failed
            processing_error: error.message, // Сохраняем текст ошибки в отдельное поле
            // Используем streaming_urls для хранения статуса
            streaming_urls: [`processing-error:${executionRecord?.$id || 'unknown'}:${new Date().toISOString()}:${error.message}`]
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

// Вспомогательная функция для безопасной отправки JSON-ответа (обновляем для совместимости)
function safeJsonResponse(res, data, isContextPattern, log, error) {
  // Используем новую улучшенную функцию
  const context = isContextPattern ? { res } : null;
  return safeResponse(res, data, context, log, error);
}