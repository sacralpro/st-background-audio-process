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

// Tracking execution progress for multi-step processing
// Удаляем это дублирующееся объявление
// const PROCESSING_STEPS = {
//   INITIALIZE: 'initialize',
//   DOWNLOAD: 'download',
//   CONVERT: 'convert',
//   SEGMENT: 'segment',
//   UPLOAD_SEGMENTS: 'upload_segments',
//   CREATE_PLAYLIST: 'create_playlist',
//   FINALIZE: 'finalize'
// };

// Main entry point for Appwrite Function - Single context parameter format
async function appwriteFunction(context) {
  // Ensure context exists and set safe defaults
  if (!context) {
    context = { req: {}, res: { json: (data) => data } };
  }
  
  // Safe wrapper for req.bodyJson to intercept errors
  if (context.req) {
    try {
      // Force define bodyJson getter that won't throw
      Object.defineProperty(context.req, 'bodyJson', {
        get: function() {
          try {
            // If body is a string, try to parse it
            if (typeof this.body === 'string') {
              try {
                return JSON.parse(this.body); // This will use our safe JSON.parse
              } catch (e) {
                console.log('[APPWRITE_FUNCTION] Safely caught bodyJson parse error');
                return {}; // Return empty object instead of throwing
              }
            }
            // If body is already an object, return it
            if (typeof this.body === 'object' && this.body !== null) {
              return this.body;
            }
            // Default
            return {};
          } catch (e) {
            console.log('[APPWRITE_FUNCTION] Safely caught bodyJson access error');
            return {}; // Return empty object instead of throwing
          }
        },
        configurable: true
      });
    } catch (e) {
      console.error('[APPWRITE_FUNCTION] Cannot redefine bodyJson property:', e.message);
    }
  }
  
  // Start tracking execution time
  const startTime = Date.now();
  
  // Initialize a safe execution status record
  let executionRecord = {
    timestamp: new Date().toISOString(),
    status: 'started',
    trigger_type: 'unknown',
    error: null,
    parsing_error: null,
    request_info: {},
    execution_time_ms: 0
  };
  
  // Log the full context type and structure for debugging
  console.log('[APPWRITE_FUNCTION] Context type:', typeof context);
  console.log('[APPWRITE_FUNCTION] Context keys:', context ? Object.keys(context).join(', ') : 'undefined');
  
  // Safely extract functions from context with fallbacks
  const req = context.req || {};
  
  // Pre-process req.body early to prevent JSON parsing errors later
  if (req.body !== undefined && typeof req.body === 'string') {
    try {
      // First, sanitize the string body
      const sanitizedBody = req.body
        .replace(/^\uFEFF/, '')                  // BOM
        .replace(/^\s+|\s+$/g, '')               // Leading/trailing whitespace
        .replace(/\u200B/g, '')                  // Zero-width space
        .replace(/\t/g, ' ')                     // Replace tabs with spaces
        .replace(/\r?\n/g, '')                   // Remove all newlines
        .replace(/[\x00-\x1F\x7F-\x9F]/g, '');   // Control characters
      
      // Find valid JSON object or array in the string
      let jsonContent = sanitizedBody;
      const firstBrace = sanitizedBody.indexOf('{');
      const lastBrace = sanitizedBody.lastIndexOf('}');
      const firstBracket = sanitizedBody.indexOf('[');
      const lastBracket = sanitizedBody.lastIndexOf(']');
      
      // Extract JSON if possible
      if (firstBrace >= 0 && lastBrace > firstBrace) {
        jsonContent = sanitizedBody.substring(firstBrace, lastBrace + 1);
        console.log('[APPWRITE_FUNCTION] Extracted JSON object from body');
      } else if (firstBracket >= 0 && lastBracket > firstBracket) {
        jsonContent = sanitizedBody.substring(firstBracket, lastBracket + 1);
        console.log('[APPWRITE_FUNCTION] Extracted JSON array from body');
      }
      
      // Attempt to parse sanitized content
      if ((jsonContent.startsWith('{') && jsonContent.endsWith('}')) || 
          (jsonContent.startsWith('[') && jsonContent.endsWith(']'))) {
        try {
          // Create a pre-parsed body that will be used later
          req.preParsedBody = JSON.parse(jsonContent);
          console.log('[APPWRITE_FUNCTION] Successfully pre-parsed request body as JSON');
        } catch (e) {
          console.error('[APPWRITE_FUNCTION] Failed to pre-parse body as JSON:', e.message);
          req.jsonParseError = e.message;
          req.sanitizedBodyContent = jsonContent;
        }
      } else {
        console.log('[APPWRITE_FUNCTION] Body is not a valid JSON structure');
        req.sanitizedBodyContent = jsonContent;
      }
    } catch (e) {
      console.error('[APPWRITE_FUNCTION] Error during req.body pre-processing:', e.message);
    }
  }
  
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
  
  // Store basic request info
  executionRecord.request_info = {
    method: req.method || 'unknown',
    url: req.url || 'unknown',
    content_type: req.headers ? (req.headers['content-type'] || 'none') : 'none',
    trigger: req.headers ? (req.headers['x-appwrite-trigger'] || 'none') : 'none'
  };
  
  executionRecord.trigger_type = executionRecord.request_info.trigger;
  
  log('- Method:', executionRecord.request_info.method);
  log('- URL:', executionRecord.request_info.url);
  if (req.headers) {
    log('- Content-Type:', executionRecord.request_info.content_type);
    log('- Trigger:', executionRecord.request_info.trigger);
  }
  
  // Initialize payload and postId as empty at the function level to avoid undefined errors
  let payload = {};
  let postId = null;
  
  // Special case handling for webhook events
  if (req.headers && req.headers['x-appwrite-trigger'] === 'event') {
    log('Detected webhook event trigger');
    
    // For webhook events, try to extract directly from headers
    if (req.headers['x-appwrite-event'] && req.headers['x-appwrite-event'].includes('.create')) {
      log('Webhook event type:', req.headers['x-appwrite-event']);
      
      // If this is a creation event, we can often extract the ID directly
      const eventResourceParts = req.headers['x-appwrite-event'].split('.');
      if (eventResourceParts.length >= 2) {
        const resourceType = eventResourceParts[0]; // e.g. 'databases.documents'
        log('Webhook resource type:', resourceType);
      }
    }
    
    // Try to extract ID from any webhook event
    if (req.preParsedBody && req.preParsedBody.$id) {
      postId = req.preParsedBody.$id;
      log(`Extracted postId from webhook event body: ${postId}`);
    }
  }
  
  try {
    // Parse request body and extract payload
    
    // DEBUG: Log the full raw request body for debugging
    if (req.body !== undefined) {
      log('DEBUG - Request body exists');
      
      // Detailed logging for debugging
      log('Request body type:', typeof req.body);
      if (typeof req.body === 'string') {
        if (req.body.length < 1000) {
          log('Raw request body:', req.body);
        } else {
          log('Raw request body (truncated):', req.body.substring(0, 500) + '...');
        }
      } else {
        try {
          log('Request body (JSON):', JSON.stringify(req.body).substring(0, 500) + '...');
        } catch (e) {
          log('Request body: [Cannot stringify body]');
        }
      }
      
      // Safe parse body - use pre-parsed body if available
      if (req.preParsedBody) {
        payload = req.preParsedBody;
        log('Using pre-parsed body payload');
      } else {
        try {
          // Safe parse body
          try {
            // Explicitly log the raw body for debugging
            if (typeof req.body === 'string') {
              log('Raw body as string:', req.body);
              
              // Check for non-printable characters
              const hasNonPrintable = /[\x00-\x1F\x7F-\x9F]/.test(req.body);
              if (hasNonPrintable) {
                log('WARNING: Non-printable characters detected in the body');
                // Show hex representation for debugging
                const hexBody = Array.from(req.body).map(c => c.charCodeAt(0).toString(16).padStart(2, '0')).join(' ');
                log('Body as hex (first 100 bytes):', hexBody.substring(0, 300));
              }
            } else {
              log('Raw body type:', typeof req.body);
            }
            
            if (typeof req.body === 'string') {
              log('Body is string, length:', req.body.length);
              if (req.body.trim().length > 0) {
                try {
                  // Try to safely parse the JSON with more aggressive cleaning
                  // Strip BOM, whitespace, and other common problematic characters
                  let cleanedBody = req.body
                    .replace(/^\uFEFF/, '')                  // BOM
                    .replace(/^\s+|\s+$/g, '')               // Leading/trailing whitespace
                    .replace(/\u200B/g, '')                  // Zero-width space
                    .replace(/[\x00-\x1F\x7F-\x9F]/g, '');   // Control characters
                  
                  // Find the first '{' and last '}' for JSON objects, or first '[' and last ']' for arrays
                  const firstBrace = cleanedBody.indexOf('{');
                  const lastBrace = cleanedBody.lastIndexOf('}');
                  const firstBracket = cleanedBody.indexOf('[');
                  const lastBracket = cleanedBody.lastIndexOf(']');
                  
                  // Determine if this looks like a JSON object or array
                  let jsonContent = cleanedBody;
                  
                  if (firstBrace >= 0 && lastBrace > firstBrace) {
                    // Extract just the JSON object
                    jsonContent = cleanedBody.substring(firstBrace, lastBrace + 1);
                    log('Extracted JSON object from content');
                  } else if (firstBracket >= 0 && lastBracket > firstBracket) {
                    // Extract just the JSON array
                    jsonContent = cleanedBody.substring(firstBracket, lastBracket + 1);
                    log('Extracted JSON array from content');
                  }
                  
                  // Check if the body is already a valid JSON string
                  if ((jsonContent.startsWith('{') && jsonContent.endsWith('}')) || 
                      (jsonContent.startsWith('[') && jsonContent.endsWith(']'))) {
                    try {
                      log('Attempting to parse cleaned JSON:', jsonContent.substring(0, 100) + (jsonContent.length > 100 ? '...' : ''));
                      payload = JSON.parse(jsonContent);
                      log('Parsed JSON payload successfully');
                    } catch (parseError) {
                      logError('JSON parse error after cleaning:', parseError.message);
                      // Try to extract postId directly if JSON parsing fails
                      const postIdMatch = jsonContent.match(/"postId"\s*:\s*"([^"]+)"/);
                      if (postIdMatch && postIdMatch[1]) {
                        payload = { postId: postIdMatch[1] };
                        log('Extracted postId from invalid JSON:', postIdMatch[1]);
                      } else {
                        payload = { postId: jsonContent };
                        log('Using cleaned content as postId fallback');
                      }
                    }
                  } else {
                    // If not a JSON object, use as raw postId
                    payload = { postId: cleanedBody };
                    log('Using cleaned body as postId (not a JSON object)');
                  }
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
        } catch (bodyError) {
          logError('Error processing body:', bodyError);
        }
      }
    }
    
    // Safely try to access bodyJson if available
    try {
      if (req.bodyJson) {
        if (typeof req.bodyJson === 'object') {
          log('bodyJson is available and is an object, using it as payload');
          payload = req.bodyJson;
        } else if (typeof req.bodyJson === 'string') {
          // Try to parse bodyJson if it's a string
          try {
            const parsedJson = JSON.parse(req.bodyJson);
            log('Parsed bodyJson string to object');
            payload = parsedJson;
          } catch (parseError) {
            logError('Failed to parse bodyJson string:', parseError.message);
          }
        } else {
          log('bodyJson is available but type is unexpected:', typeof req.bodyJson);
        }
      }
    } catch (jsonError) {
      logError('Error accessing bodyJson:', jsonError);
    }
    
    // Try to extract postId from various sources
    postId = payload.postId;
    
    // Check URL parameters
    if (!postId && req.url) {
      try {
        log('Checking URL parameters for postId');
        
        // Enhanced URL parsing with fallbacks
        let url;
        let params;
        
        try {
          // Regular URL parsing
          url = new URL(req.url);
          params = new URLSearchParams(url.search);
          log('Parsed URL:', url.toString());
          log('URL has search params:', url.search ? 'yes' : 'no');
        } catch (urlParseError) {
          // Fallback for relative URLs or non-standard URLs
          logError('Standard URL parsing failed, trying fallback:', urlParseError.message);
          
          const queryPart = req.url.split('?')[1];
          if (queryPart) {
            params = new URLSearchParams(queryPart);
            log('Parsed URL params using fallback method');
          } else {
            log('No query parameters found in URL');
            params = new URLSearchParams();
          }
        }
        
        // Check for postId in URL params
        if (params.has('postId')) {
          postId = params.get('postId');
          log(`Found postId in URL params: ${postId}`);
        } else {
          // Additional check for ID in URL path
          const pathParts = req.url.split('/');
          const lastPart = pathParts[pathParts.length - 1];
          
          // Check if last part could be an ID (no slashes, reasonable length)
          if (lastPart && lastPart.length > 5 && lastPart.length < 30 && !lastPart.includes('?')) {
            log(`Potential ID found in URL path: ${lastPart}`);
            if (!postId) {
              postId = lastPart;
              log(`Using URL path component as postId: ${postId}`);
            }
          }
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
        log('Detected Appwrite document event');
        
        // Safe access to req.bodyJson for events
        let eventData = null;
        
        // Try multiple sources for event data
        if (req.bodyJson) {
          log('Using req.bodyJson for event data');
          eventData = req.bodyJson;
        } else if (req.preParsedBody) {
          log('Using preParsedBody for event data');
          eventData = req.preParsedBody;
        } else if (payload && typeof payload === 'object') {
          log('Using payload for event data');
          eventData = payload;
        } else if (req.sanitizedBodyContent) {
          // As a last resort, try finding ID using regex in sanitized content
          log('Trying to extract ID using regex from sanitized content');
          const idMatch = req.sanitizedBodyContent.match(/"\\?\$id\\?"\s*:\s*"([^"]+)"/);
          if (idMatch && idMatch[1]) {
            postId = idMatch[1];
            log(`Extracted postId using regex: ${postId}`);
          }
        }
        
        // Try to find document ID in multiple places
        if (eventData) {
          if (eventData.$id) {
            postId = eventData.$id;
            log(`Extracted postId from event $id: ${postId}`);
          } else if (eventData.record && eventData.record.$id) {
            postId = eventData.record.$id;
            log(`Extracted postId from event record $id: ${postId}`);
          } else if (eventData.document && eventData.document.$id) {
            postId = eventData.document.$id;
            log(`Extracted postId from event document $id: ${postId}`);
          }
        }
        
        // Log if we couldn't find an ID
        if (!postId) {
          log('Could not extract postId from event data');
          if (eventData) {
            log('Event data keys:', Object.keys(eventData || {}).join(', '));
          }
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
    
    // Sanitize postId if it's a JSON-like string
    if (typeof postId === 'string' && 
        (postId.startsWith('{') || postId.startsWith('['))) {
      log('Warning: postId appears to be a JSON string, sanitizing...');
      
      // Create a simplified ID by hashing or using a part of the string
      const sanitizedId = postId
        .replace(/[{}\[\]"'\\]/g, '')   // Remove JSON chars
        .replace(/\s+/g, '')            // Remove spaces
        .replace(/[^a-zA-Z0-9]/g, '')   // Keep only alphanumeric
        .substring(0, 20);              // Limit length
      
      if (sanitizedId.length > 5) {
        log(`Sanitized postId from ${postId.length} chars to "${sanitizedId}"`);
        postId = sanitizedId;
      }
    }
    
    log(`Processing audio for post: ${postId}`);
    
    // Get post document
    const post = await databases.getDocument(
      APPWRITE_DATABASE_ID,
      APPWRITE_COLLECTION_ID,
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
    
    // Check for continuation processing - special case when we're resuming processing
    // from a previous request that was intentionally ended early to avoid timeouts
    if (post.processing_continuation_scheduled && post.processing_stage) {
      log(`Detected continuation processing for stage: ${post.processing_stage}`);
      
      // Run the appropriate continuation based on the saved stage
      if (post.processing_stage === 'segment_upload') {
        return await processContinuationSegmentUpload(post, req, res, databases, storage, executionRecord, startTime);
      } else if (post.processing_stage === 'playlist_creation') {
        return await processContinuationPlaylistCreation(post, req, res, databases, storage, executionRecord, startTime);
      } else if (post.processing_stage === 'download') {
        return await processContinuationDownload(post, req, res, databases, storage, executionRecord, startTime);
      } else if (post.processing_stage === 'ffmpeg_process') {
        return await processContinuationFFmpeg(post, req, res, databases, storage, executionRecord, startTime);
      } else if (post.processing_stage === 'playlist_preparation') {
        return await processContinuationPlaylistPreparation(post, req, res, databases, storage, executionRecord, startTime);
      }
    }
    
    if (!post.audio_file_id) {
      return res.json({
        success: false,
        message: 'No audio file found for post'
      });
    }
    
    // Initial processing - check if we're just starting or in 'pending' state
    if (!post.processing_status || post.processing_status === 'pending') {
      // Just initialize and schedule the first continuation
      await databases.updateDocument(
        APPWRITE_DATABASE_ID,
        APPWRITE_COLLECTION_ID,
        post.$id,
        {
          processing_status: 'processing',
          processing_progress: 5,
          processing_started_at: new Date().toISOString(),
          processing_stage: 'download',
          processing_continuation_scheduled: true,
          processing_next_step_at: new Date(Date.now() + 3000).toISOString()
        }
      );
      
      // Schedule the download step to run after a short delay
      const taskId = await scheduleContinuationTask(
        { ...post, processing_stage: 'download' },
        3
      );
      
      return res.json({
        success: true,
        message: 'Audio processing initialized and scheduled',
        postId: post.$id,
        continuation_scheduled: true,
        continuation_task_id: taskId || 'none'
      });
    }
    
    // Check if this is a continuation of a previous processing attempt
    if (post.processing_status === 'processing' && post.processing_progress > 10) {
      const currentProgress = post.processing_progress || 0;
      log(`Resuming processing for post ${post.$id} from progress: ${currentProgress}%`);
      
      // If processing is already completed, return success
      if (post.processing_status === 'completed') {
        log(`Post ${post.$id} is already processed, skipping`);
        return res.json({
          success: true,
          message: 'Audio processing already completed',
          postId: post.$id,
          mp3_file_id: post.mp3_file_id,
          hls_playlist_id: post.hls_playlist_id
        });
      }
    }
    
    // Rate limiter/debouncer for repeated requests
    // Check if we've recently started processing this post (within 30 seconds)
    const now = new Date();
    const processingStartedAt = post.processing_started_at ? new Date(post.processing_started_at) : null;
    
    if (processingStartedAt && 
        (now - processingStartedAt) < 30000 && // 30 seconds
        post.processing_status === 'processing') {
      log(`Post ${post.$id} is already being processed (started ${Math.round((now - processingStartedAt)/1000)}s ago), preventing duplicate processing`);
      return res.json({
        success: true, 
        message: 'Audio processing already in progress',
        postId: post.$id,
        processing_status: post.processing_status,
        processing_progress: post.processing_progress || 0
      });
    }
    
    // Update post status to processing
    await databases.updateDocument(
      APPWRITE_DATABASE_ID,
      APPWRITE_COLLECTION_ID,
      post.$id,
      {
        processing_status: 'processing',
        processing_progress: 10,
        processing_started_at: new Date().toISOString()
      }
    );
    
    log(`Updated post ${post.$id} status to processing`);
    
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
      
      // Check if MP3 is already uploaded or create a new one
      let mp3UploadResult;
      if (post.mp3_file_id) {
        log(`Using existing MP3 file: ${post.mp3_file_id}`);
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
        
        log(`Uploaded MP3 file, ID: ${mp3UploadResult.$id}`);
        
        // Immediately update post with MP3 ID to prevent re-uploading on timeout
        await databases.updateDocument(
          APPWRITE_DATABASE_ID,
          APPWRITE_COLLECTION_ID,
          post.$id,
          {
            mp3_file_id: mp3UploadResult.$id,
            processing_progress: 30
          }
        );
      }
      
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
        APPWRITE_COLLECTION_ID,
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
        APPWRITE_COLLECTION_ID,
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
        APPWRITE_COLLECTION_ID,
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
        APPWRITE_COLLECTION_ID,
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
      
      // Update execution record
      executionRecord.status = 'completed';
      executionRecord.execution_time_ms = Date.now() - startTime;
      
      // Log execution attempt to database regardless of success/failure
      try {
        // Create a processing event record if we have client connection
        if (databases && APPWRITE_DATABASE_ID) {
          const recordId = ID.unique();
          const eventData = {
            function_id: process.env.APPWRITE_FUNCTION_ID || 'unknown',
            execution_id: recordId,
            timestamp: executionRecord.timestamp,
            status: executionRecord.status,
            error: executionRecord.error,
            trigger_type: executionRecord.trigger_type,
            request_info: JSON.stringify(executionRecord.request_info),
            post_id: post ? post.$id : (postId || 'unknown'),
            execution_time_ms: executionRecord.execution_time_ms
          };
          
          try {
            // Only try to log if logs collection exists - don't break execution if it doesn't
            // Check if collection exists first or use a known existing collection
            if (process.env.APPWRITE_COLLECTION_ID_LOGS) {
              await databases.createDocument(
                APPWRITE_DATABASE_ID,
                process.env.APPWRITE_COLLECTION_ID_LOGS,
                recordId,
                eventData
              );
              log(`Logged execution event: ${recordId}`);
            } else {
              // Optional fallback - update post with execution info if available
              if (post && post.$id) {
                await databases.updateDocument(
                  APPWRITE_DATABASE_ID,
                  APPWRITE_COLLECTION_ID,
                  post.$id,
                  {
                    last_execution_info: JSON.stringify({
                      timestamp: executionRecord.timestamp,
                      status: executionRecord.status,
                      execution_time_ms: executionRecord.execution_time_ms
                    })
                  }
                );
                log('Logged execution summary to post document');
              }
            }
          } catch (logError) {
            // Just log the error, don't break execution
            console.error('Failed to log execution event:', logError);
          }
        }
      } catch (logError) {
        console.error('Failed to create execution log:', logError);
      }
      
      // Ensure postId is defined before using it
      const safePostId = (post ? post.$id : null) || postId || (payload && payload.postId) || null;
      
      if (safePostId) {
        await databases.updateDocument(
          APPWRITE_DATABASE_ID,
          APPWRITE_COLLECTION_ID,
          safePostId,
          {
            processing_status: 'completed',
            processing_progress: 100,
            processing_completed_at: new Date().toISOString()
          }
        );
        log(`Updated post ${safePostId} with completed status`);
      } else {
        log('Cannot update post with completed status: postId is not defined');
      }
      
      // Explicitly return early with a success response if we expect further processing
      // This prevents the Cloudflare 524 timeout by intentionally ending the request
      // while processing continues in Appwrite background tasks
      if (segmentFiles.length > 10) {
        log(`Large audio file detected (${segmentFiles.length} segments). Returning early while processing continues`);
        
        // Calculate task delay with small jitter to prevent concurrent runs
        const delaySeconds = 5 + Math.floor(Math.random() * 3); // 5-8 second delay
        
        // Schedule a follow-up by setting a special status
        await databases.updateDocument(
          APPWRITE_DATABASE_ID,
          APPWRITE_COLLECTION_ID,
          post.$id,
          {
            processing_status: 'processing',
            processing_progress: 75,
            mp3_file_id: mp3UploadResult.$id,
            hls_segment_ids: segmentIds.slice(0, 5), // Store first few segment IDs
            processing_stage: 'segment_upload',
            segment_files_remaining: segmentFiles.length - 5, // Process first 5 segments
            segment_files_processed: 5,
            processing_continuation_scheduled: true,
            processing_next_step_at: new Date(Date.now() + (delaySeconds * 1000)).toISOString()
          }
        );
        
        // Attempt to schedule continuation task
        const taskId = await scheduleContinuationTask(post, delaySeconds);
        
        // Return early success
        return res.json({
          success: true,
          message: 'Audio processing in progress - large file handling enabled',
          postId: post.$id,
          mp3_file_id: mp3UploadResult.$id,
          continuation_scheduled: true,
          continuation_task_id: taskId || 'none'
        });
      }
      
      // Return success response
      return res.json({
        success: true,
        message: 'Audio processing completed',
        postId: safePostId,
        mp3_file_id: mp3UploadResult.$id,
        hls_playlist_id: playlistUploadResult.$id
      });
    } catch (error) {
      logError('Error processing audio:', error);
      
      // Update execution record
      executionRecord.status = 'error';
      executionRecord.error = error.message;
      executionRecord.execution_time_ms = Date.now() - startTime;
      
      try {
        // Log execution attempt to database regardless of success/failure
        try {
          // Create a processing event record if we have client connection
          if (databases && APPWRITE_DATABASE_ID) {
            const recordId = ID.unique();
            const eventData = {
              function_id: process.env.APPWRITE_FUNCTION_ID || 'unknown',
              execution_id: recordId,
              timestamp: executionRecord.timestamp,
              status: executionRecord.status,
              error: executionRecord.error,
              trigger_type: executionRecord.trigger_type,
              request_info: JSON.stringify(executionRecord.request_info),
              post_id: post ? post.$id : (postId || 'unknown'),
              execution_time_ms: executionRecord.execution_time_ms
            };
            
            try {
              // Only try to log if logs collection exists - don't break execution if it doesn't
              // Check if collection exists first or use a known existing collection
              if (process.env.APPWRITE_COLLECTION_ID_LOGS) {
                await databases.createDocument(
                  APPWRITE_DATABASE_ID,
                  process.env.APPWRITE_COLLECTION_ID_LOGS,
                  recordId,
                  eventData
                );
                log(`Logged execution event: ${recordId}`);
              } else {
                // Optional fallback - update post with execution info if available
                if (post && post.$id) {
                  await databases.updateDocument(
                    APPWRITE_DATABASE_ID,
                    APPWRITE_COLLECTION_ID,
                    post.$id,
                    {
                      last_execution_info: JSON.stringify({
                        timestamp: executionRecord.timestamp,
                        status: executionRecord.status,
                        execution_time_ms: executionRecord.execution_time_ms
                      })
                    }
                  );
                  log('Logged execution summary to post document');
                }
              }
            } catch (logError) {
              // Just log the error, don't break execution
              console.error('Failed to log execution event:', logError);
            }
          }
        } catch (logError) {
          console.error('Failed to create execution log:', logError);
        }
        
        // Ensure postId is defined before using it
        const safePostId = (post ? post.$id : null) || postId || (payload && payload.postId) || null;
        
        if (safePostId) {
          await databases.updateDocument(
            APPWRITE_DATABASE_ID,
            APPWRITE_COLLECTION_ID,
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
    
    // Update execution record
    executionRecord.status = 'error';
    executionRecord.error = error.message;
    executionRecord.execution_time_ms = Date.now() - startTime;
    
    try {
      // Log execution attempt to database regardless of success/failure
      try {
        // Create a processing event record if we have client connection
        if (databases && APPWRITE_DATABASE_ID) {
          const recordId = ID.unique();
          const eventData = {
            function_id: process.env.APPWRITE_FUNCTION_ID || 'unknown',
            execution_id: recordId,
            timestamp: executionRecord.timestamp,
            status: executionRecord.status,
            error: executionRecord.error,
            trigger_type: executionRecord.trigger_type,
            request_info: JSON.stringify(executionRecord.request_info),
            post_id: postId || 'unknown',
            execution_time_ms: executionRecord.execution_time_ms
          };
          
          try {
            // Only try to log if logs collection exists - don't break execution if it doesn't
            // Check if collection exists first or use a known existing collection
            if (process.env.APPWRITE_COLLECTION_ID_LOGS) {
              await databases.createDocument(
                APPWRITE_DATABASE_ID,
                process.env.APPWRITE_COLLECTION_ID_LOGS,
                recordId,
                eventData
              );
              log(`Logged execution event: ${recordId}`);
            } else {
              // Optional fallback - update post with execution info if available
              if (post && post.$id) {
                await databases.updateDocument(
                  APPWRITE_DATABASE_ID,
                  APPWRITE_COLLECTION_ID,
                  post.$id,
                  {
                    last_execution_info: JSON.stringify({
                      timestamp: executionRecord.timestamp,
                      status: executionRecord.status,
                      execution_time_ms: executionRecord.execution_time_ms
                    })
                  }
                );
                log('Logged execution summary to post document');
              }
            }
          } catch (logError) {
            // Just log the error, don't break execution
            console.error('Failed to log execution event:', logError);
          }
        }
      } catch (logError) {
        console.error('Failed to create execution log:', logError);
      }
      
      // Ensure postId is defined before using it
      const safePostId = postId || (payload && payload.postId) || null;
      
      if (safePostId) {
        await databases.updateDocument(
          APPWRITE_DATABASE_ID,
          APPWRITE_COLLECTION_ID,
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

// Helper function to schedule the next step
async function scheduleNextStep(client, executionId, postId) {
  try {
    // Get the Appwrite function endpoint
    const functionEndpoint = process.env.APPWRITE_FUNCTION_ENDPOINT || 'https://cloud.appwrite.io/v1/functions';
    const functionId = process.env.APPWRITE_FUNCTION_ID;
    
    if (!functionId) {
      throw new Error('APPWRITE_FUNCTION_ID environment variable is not set');
    }
    
    // Create axios instance with API key
    const apiClient = axios.create({
      headers: {
        'X-Appwrite-Key': process.env.APPWRITE_API_KEY,
        'Content-Type': 'application/json'
      }
    });
    
    // Schedule the next step
    const executionUrl = `${functionEndpoint}/${functionId}/executions`;
    const payload = {
      async: true,
      data: JSON.stringify({
        type: 'continue_execution',
        executionId,
        postId
      })
    };
    
    console.log(`Scheduling next step for execution ${executionId}`, payload);
    
    const response = await apiClient.post(executionUrl, payload);
    
    console.log(`Scheduled next step, response status: ${response.status}`);
    return response.data;
  } catch (error) {
    console.error(`Failed to schedule next step for execution ${executionId}:`, error);
    throw error;
  }
}

// Main function
module.exports = async function(req, res) {
  // Подробное логирование для отладки
  console.log("=== REQUEST DEBUG INFO ===");
  console.log("Request type:", typeof req);
  console.log("Request keys:", req ? Object.keys(req).join(', ') : 'undefined');
  console.log("Headers:", req.headers ? JSON.stringify(req.headers) : 'undefined');
  
  // Улучшенное извлечение payload
  let payload = {};
  let rawBody = null;
  
  try {
    // Логируем сырые данные
    console.log("Raw body type:", typeof req.body);
    console.log("Raw body:", req.body);
    
    // Вариант 1: req.body строка JSON
    if (req.body && typeof req.body === 'string') {
      try {
        rawBody = req.body;
        payload = JSON.parse(req.body);
        console.log("Parsed payload from string:", payload);
      } catch (e) {
        console.error("Error parsing body string:", e.message);
      }
    }
    // Вариант 2: req.body уже объект
    else if (req.body && typeof req.body === 'object') {
      payload = req.body;
      console.log("Using body object directly:", payload);
      
      // Проверка на специальный формат Appwrite с полем data
      if (payload.data && typeof payload.data === 'string') {
        try {
          const innerData = JSON.parse(payload.data);
          console.log("Found nested data in payload:", innerData);
          payload = innerData;
        } catch (e) {
          console.error("Error parsing nested data:", e.message);
        }
      }
    }
    // Вариант 3: req.bodyJson (в некоторых версиях Appwrite)
    else if (req.bodyJson) {
      payload = req.bodyJson;
      console.log("Using bodyJson:", payload);
    }
    
    // Вариант 4: Проверяем req.payload (в некоторых версиях)
    else if (req.payload) {
      payload = req.payload;
      console.log("Using req.payload:", payload);
    }
    
    // Получаем postId из разных возможных мест
    let postId = null;
    
    if (payload.postId) {
      postId = payload.postId;
      console.log("PostId from payload:", postId);
    } 
    else if (payload.post_id) {
      postId = payload.post_id;
      console.log("PostId from payload.post_id:", postId);
    }
    else if (payload.id) {
      postId = payload.id;
      console.log("PostId from payload.id:", postId);
    }
    // Проверка на данные из вебхуков
    else if (payload.webhook && payload.webhook.payload && payload.webhook.payload.$id) {
      postId = payload.webhook.payload.$id;
      console.log("PostId from webhook payload:", postId);
    }
    
    console.log("Final postId to be used:", postId);
    
    // Starting audio processing function
    console.log('Starting audio processing function');
    
    // Identify trigger type
    let triggerType = 'direct';
    let executionId = null;
    
    // Check if triggered by schedule
    if (req.headers && req.headers['x-appwrite-trigger'] === 'schedule') {
      triggerType = 'schedule';
      console.log('Triggered by schedule');
    }
    
    // Check if triggered by webhook
    else if (payload.webhook) {
      triggerType = 'webhook';
      console.log('Webhook event:', payload.webhook);
    }
    
    // Check if continuation of execution
    else if (payload.type === 'continue_execution') {
      triggerType = 'continuation';
      executionId = payload.executionId;
      postId = payload.postId;
      console.log(`Continuing execution ${executionId} for post ${postId}`);
    }
    
    // Otherwise assume direct API call
    else {
      console.log(`Direct API call for post ${postId}`);
    }
    
    // Process based on trigger type
    let result = null;
    
    if (triggerType === 'schedule') {
      // Process unprocessed posts
      result = await processUnprocessedPosts(databases, storage, client);
    }
    else if (triggerType === 'webhook') {
      try {
        // Get post ID from webhook
        const event = payload.webhook;
        console.log('Webhook event:', event);
        
        // Check if it's a database event and has valid post ID
        if (event.event && event.event.includes('databases') && event.payload && event.payload.$id) {
          // Process the post
          postId = event.payload.$id;
          result = await processAudio(postId, databases, storage, client);
        } else {
          console.error('Invalid webhook event:', event);
          result = { success: false, message: 'Invalid webhook event' };
        }
      } catch (webhookError) {
        console.error('Error processing webhook:', webhookError);
        result = { success: false, message: `Error processing webhook: ${webhookError.message}` };
      }
    }
    else if (triggerType === 'continuation') {
      // Continue processing the post
      result = await processAudio(postId, databases, storage, client, executionId);
      
      // Schedule next step if needed
      if (result.success && result.currentStep) {
        await scheduleNextStep(client, result.executionId, result.postId);
        console.log(`Scheduled next step: ${result.currentStep}`);
      }
    }
    else if (postId) {
      // Direct API call with post ID
      result = await processAudio(postId, databases, storage, client);
      
      // Schedule next step if needed
      if (result.success && result.currentStep) {
        await scheduleNextStep(client, result.executionId, result.postId);
        console.log(`Scheduled next step: ${result.currentStep}`);
      }
    }
    else {
      console.error('No post ID provided');
      result = { success: false, message: 'No post ID provided' };
    }
    
    // Send response if available
    if (res && typeof res.json === 'function') {
      console.log('Sending response:', result);
      return res.json(result);
    }
    
    return result;
  } catch (error) {
    console.error('Error in audio processing function:', error);
    
    // Send error response if available
    if (res && typeof res.json === 'function') {
      return res.json({
        success: false,
        message: `Error in audio processing function: ${error.message}`,
        postId: payload.postId
      });
    }
    
    return {
      success: false,
      message: `Error in audio processing function: ${error.message}`,
      postId: payload.postId
    };
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
    
    console.log(`Found ${posts.total} unprocessed posts`);
    
    // Process each post
    const results = [];
    for (const post of posts.documents) {
      console.log(`Processing unprocessed post: ${post.$id}`);
      
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
        console.log(`Scheduled next step: ${result.currentStep}`);
      }
    }
    
    return {
      success: true,
      message: `Processed ${results.length} posts`,
      results
    };
  } catch (error) {
    console.error('Error fetching unprocessed posts:', error);
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
    
    console.log(`Updated UI progress for post ${postId}: ${step} - ${progressPercentage}%`);
    return true;
  } catch (error) {
    console.error(`Failed to update UI progress for post ${postId}:`, error);
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
      console.log('No progress webhook URL configured, skipping notification');
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
    
    console.log(`Sent progress webhook for post ${postId}: ${response.status}`);
    return true;
  } catch (error) {
    console.error(`Failed to send progress webhook for post ${postId}:`, error);
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
  console.log(`${logPrefix} Starting download process`);
  
  try {
    // Create a temp directory for processing
    const tempDir = path.join('/tmp', `audio-processing-${post.$id}`);
    try {
      await mkdirAsync(tempDir, { recursive: true });
      console.log(`${logPrefix} Created temp directory: ${tempDir}`);
    } catch (err) {
      if (err.code !== 'EEXIST') {
        throw err;
      }
    }
    
    // Download the audio file
    const audioFileName = `${post.$id}.mp3`;
    const audioFilePath = path.join(tempDir, audioFileName);
    
    console.log(`${logPrefix} Downloading audio file ${post.audio_file_id} to ${audioFilePath}`);
    
    // Get the file data
    const fileData = await storage.getFileDownload(
      APPWRITE_BUCKET_ID,
      post.audio_file_id
    );
    
    // Save the file to disk
    await writeFileAsync(audioFilePath, Buffer.from(fileData));
    
    console.log(`${logPrefix} Downloaded and saved audio file to ${audioFilePath}`);
    
    // Check if MP3 is already uploaded or create a new one
    let mp3UploadResult;
    if (post.mp3_file_id) {
      console.log(`${logPrefix} Using existing MP3 file: ${post.mp3_file_id}`);
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
      
      console.log(`${logPrefix} Uploaded MP3 file, ID: ${mp3UploadResult.$id}`);
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
    console.error(`${logPrefix} Error:`, error);
    
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
      console.error(`${logPrefix} Failed to update post with error:`, updateError);
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
  console.log(`${logPrefix} Starting FFmpeg processing`);
  
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
    console.log(`${logPrefix} Creating HLS segments...`);
    
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
          console.log(`${logPrefix} Processing: ${JSON.stringify(progress)}`);
        })
        .on('end', () => {
          console.log(`${logPrefix} HLS segmentation completed`);
          resolve();
        })
        .on('error', err => {
          console.error(`${logPrefix} Error during HLS segmentation:`, err);
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
    console.error(`${logPrefix} Error:`, error);
    
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
      console.error(`${logPrefix} Failed to update post with error:`, updateError);
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
  console.log(`${logPrefix} Starting playlist preparation`);
  
  try {
    // Get paths from post data
    const tempDir = post.temp_dir || path.join('/tmp', `audio-processing-${post.$id}`);
    const hlsDir = post.hls_dir || path.join(tempDir, 'hls');
    const m3u8Path = post.m3u8_path || path.join(hlsDir, 'playlist.m3u8');
    
    // Read the generated playlist
    const playlistContent = await readFileAsync(m3u8Path, 'utf8');
    console.log(`${logPrefix} Read playlist: ${m3u8Path}`);
    
    // Get segment files
    const segmentFiles = fs.readdirSync(hlsDir).filter(file => file.endsWith('.aac'));
    console.log(`${logPrefix} Found ${segmentFiles.length} segment files`);
    
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
        
        console.log(`${logPrefix} Uploaded segment ${segmentFile}, ID: ${segmentUploadResult.$id}`);
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
    console.error(`${logPrefix} Error:`, error);
    
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
      console.error(`${logPrefix} Failed to update post with error:`, updateError);
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
      
      console.log(`[SCHEDULER] Creating continuation task for post ${post.$id} with ${delaySeconds}s delay`);
      
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
      
      console.log(`[SCHEDULER] Created continuation task, execution ID: ${execution.$id}`);
      
      return execution.$id;
    } else {
      console.log('[SCHEDULER] Task scheduling disabled due to missing environment variables');
      return null;
    }
  } catch (error) {
    console.error('[SCHEDULER] Error scheduling continuation task:', error);
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
    console.error('Error in segment upload continuation:', error);
    
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
      console.error('Failed to update error status in database:', dbError);
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
      console.error('Error cleaning up temporary files:', cleanupError);
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
    console.error('Error in playlist creation:', error);
    
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
      console.error('Failed to update error status in database:', dbError);
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
        console.error('Error cleaning up temporary files:', cleanupError);
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
    console.error('Error in cleanup and execution update:', error);
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
    console.error(`Error managing execution for post ${postId}:`, error);
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
    console.error(`Error checking execution limits for post ${postId}:`, err);
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
  const log = (message) => console.log(`[${executionRecord.$id}] ${message}`);
  const logError = (message, error) => console.error(`[${executionRecord.$id}] ${message}`, error);
  
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