// Скрипт для локального тестирования функции Appwrite
const dotenv = require('dotenv');
const appwriteFunction = require('./src/appwriteFunction');

// Загрузка переменных окружения из .env файла
dotenv.config();

// Мок-объект для req и res
const req = {
  body: JSON.stringify({
    postId: 'ваш_идентификатор_поста' // Замените на реальный ID поста для тестирования
  }),
  log: (...args) => console.log('[TEST LOG]', ...args),
  error: (...args) => console.error('[TEST ERROR]', ...args)
};

const res = {
  json: (data) => {
    console.log('\n[RESPONSE]', JSON.stringify(data, null, 2));
    return data;
  }
};

// Запуск функции
async function runTest() {
  console.log('Starting local test...');
  try {
    const result = await appwriteFunction(req, res);
    console.log('Test completed');
  } catch (error) {
    console.error('Test failed with error:', error);
  }
}

runTest(); 