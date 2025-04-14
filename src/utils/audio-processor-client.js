/**
 * Утилита для вызова Appwrite функции обработки аудио
 * 
 * Используйте этот модуль в вашем Next.js приложении Sacral Track
 * для запуска обработки аудио сразу после создания поста.
 */

import { Client, Functions } from 'appwrite';

// Константы для работы с Appwrite
const APPWRITE_ENDPOINT = process.env.NEXT_PUBLIC_APPWRITE_ENDPOINT || 'https://cloud.appwrite.io/v1';
const APPWRITE_PROJECT_ID = process.env.NEXT_PUBLIC_APPWRITE_PROJECT_ID || '67f223590032b871e5f6';
const AUDIO_PROCESSOR_FUNCTION_ID = process.env.NEXT_PUBLIC_APPWRITE_FUNCTION_ID || '67fd5f3793f097add368';

/**
 * Запустить обработку аудио для указанного поста
 * 
 * @param {string} postId - ID поста для обработки
 * @param {Object} options - Дополнительные опции
 * @returns {Promise<Object>} Результат выполнения
 */
export async function startAudioProcessing(postId, options = {}) {
    if (!postId) {
        throw new Error('Post ID is required to start audio processing');
    }

    try {
        // Инициализируем Appwrite Client
        const client = new Client()
            .setEndpoint(APPWRITE_ENDPOINT)
            .setProject(APPWRITE_PROJECT_ID);

        // Если пользователь аутентифицирован, используем его сессию
        // В противном случае функция будет выполнена с правами гостя
        if (options.session) {
            client.setSession(options.session);
        }

        // Инициализируем Functions API
        const functions = new Functions(client);

        // Выполняем функцию
        const execution = await functions.createExecution(
            AUDIO_PROCESSOR_FUNCTION_ID, 
            JSON.stringify({ postId }),
            false, // асинхронное выполнение (true = асинхронно, false = синхронно)
            '/',   // путь 
            'POST' // метод
        );

        console.log('Audio processing started:', execution);

        // Возвращаем результат
        return {
            success: true,
            executionId: execution.$id,
            statusCode: execution.statusCode,
            response: execution.response,
            message: 'Audio processing initiated'
        };
    } catch (error) {
        console.error('Error starting audio processing:', error);
        return {
            success: false,
            error: error.message,
            message: 'Failed to start audio processing'
        };
    }
}

/**
 * Проверить статус обработки аудио для указанного поста
 * @param {string} postId - ID поста для проверки
 * @returns {Promise<Object>} Информация о статусе
 */
export async function checkAudioProcessingStatus(postId) {
    // Здесь можно реализовать логику проверки статуса обработки
    // путем запроса документа из базы данных и проверки полей
    // processing_status, mp3_url, и т.д.
    
    // Для этого понадобится использовать Databases API Appwrite
    // Реализация будет зависеть от структуры вашего приложения
}

/**
 * Пример использования в компоненте Next.js при создании поста:
 * 
 * import { startAudioProcessing } from '../utils/audio-processor-client';
 * 
 * // В обработчике отправки формы
 * const handleSubmit = async (e) => {
 *   e.preventDefault();
 *   
 *   // Загружаем аудиофайл и создаем пост...
 *   const newPost = await createPost(...);
 *   
 *   // Запускаем обработку аудио
 *   const result = await startAudioProcessing(newPost.$id);
 *   
 *   if (result.success) {
 *     toast.success('Пост создан, аудио обрабатывается');
 *   } else {
 *     toast.warning('Пост создан, но обработка аудио не запущена');
 *   }
 * };
 */ 