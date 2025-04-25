// Скрипт для тестирования функции через Appwrite API
const { Client, Functions } = require('node-appwrite');
const dotenv = require('dotenv');

// Загрузка переменных окружения из .env файла
dotenv.config();

// Инициализация клиента Appwrite
const client = new Client()
    .setEndpoint(process.env.APPWRITE_ENDPOINT)
    .setProject(process.env.APPWRITE_FUNCTION_PROJECT_ID)
    .setKey(process.env.APPWRITE_API_KEY);

const functions = new Functions(client);

// Параметры теста
const functionId = process.env.APPWRITE_FUNCTION_ID; // ID вашей функции в Appwrite
const testPostId = process.env.TEST_POST_ID; // ID поста для тестирования

async function testFunction() {
    console.log(`Тестирование функции ${functionId} с постом ${testPostId}`);
    
    try {
        // Создаем выполнение функции с тестовыми данными
        const execution = await functions.createExecution(
            functionId,
            JSON.stringify({
                postId: testPostId
            }),
            false // true для асинхронного выполнения
        );
        
        console.log(`Выполнение запущено: ${execution.$id}`);
        console.log('Статус:', execution.status);
        console.log('Ответ:', execution.response);
        
        // Если была ошибка
        if (execution.errors && execution.errors.length > 0) {
            console.error('Ошибки:', execution.errors);
        }
        
        return execution;
    } catch (error) {
        console.error('Ошибка при выполнении функции:', error);
        throw error;
    }
}

// Запуск теста
testFunction()
    .then(() => console.log('Тест завершен'))
    .catch(err => console.error('Тест не удался:', err)); 