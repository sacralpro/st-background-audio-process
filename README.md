# Sacral Track Background Audio Processor

Фоновый сервис для обработки аудиофайлов в социальной сети Sacral Track. Приложение автоматически конвертирует WAV аудиофайлы в MP3 и создает HLS-плейлисты для потокового воспроизведения.

## Функциональность

- Мониторинг новых постов с WAV аудиофайлами в Appwrite
- Конвертация WAV в MP3
- Разбиение MP3 на сегменты для HLS-потоков
- Создание m3u8 плейлистов
- Загрузка обработанных файлов в Appwrite Storage
- Обновление записей в базе данных

## Варианты развертывания

### 1. На Netlify с регулярным запуском

Приложение настроено для запуска как сервис Netlify с регулярными проверками:
- Каждые 15 минут запускается проверка новых аудиофайлов
- Webhook-эндпоинт для обработки постов в реальном времени

### 2. Как Appwrite Function

Включен шаблон Appwrite Function, который можно подключить напрямую к коллекции постов для обработки аудио при создании или обновлении записи.

## Требования

- Node.js 16.x или выше
- Appwrite аккаунт и проект
- FFmpeg (устанавливается автоматически через NPM)
- API ключ Appwrite с правами на чтение/запись в БД и Storage

## Установка

1. Клонировать репозиторий:
```bash
git clone https://github.com/yourusername/st-background-audio-process.git
cd st-background-audio-process
```

2. Установить зависимости:
```bash
npm install
```

3. Создать файл .env с переменными окружения:
```
APPWRITE_ENDPOINT=https://cloud.appwrite.io/v1
APPWRITE_PROJECT_ID=your_project_id
APPWRITE_API_KEY=your_api_key
APPWRITE_DATABASE_ID=your_database_id
APPWRITE_COLLECTION_ID_POST=your_collection_id
APPWRITE_BUCKET_ID=your_bucket_id
CHECK_INTERVAL=60000
```

## Запуск

### Локальный запуск
```bash
npm start
```

### Для разработки с автоматической перезагрузкой
```bash
npm run dev
```

## Развертывание на Netlify

1. Подключить репозиторий к Netlify
2. Настроить переменные окружения в настройках проекта Netlify
3. Deploying будет выполнен автоматически

### Настройка вебхуков в Appwrite

1. В консоли Appwrite перейдите в свой проект → Database → Collection → Posts
2. Создайте новый Webhook для событий "Create Document" и "Update Document"
3. Укажите URL вашей Netlify функции: `https://your-netlify-site.netlify.app/.netlify/functions/webhook-processor`

## Настройка Appwrite Function

Для использования варианта с Appwrite Function:

1. В консоли Appwrite перейдите в Functions → Create function
2. Выберите Node.js runtime
3. Скопируйте содержимое файла `src/appwriteFunction.js` в редактор кода
4. Настройте переменные окружения
5. Настройте триггер для запуска функции при создании/обновлении документа

## Структура проекта

```
├── functions/                # Netlify функции
│   ├── scheduled-processor.js # Функция для регулярного запуска
│   └── webhook-processor.js   # Функция для обработки вебхуков
├── src/
│   ├── appwrite.js           # Интеграция с Appwrite API
│   ├── audioProcessor.js     # Функции для обработки аудио
│   ├── processor.js          # Основная логика обработки
│   └── appwriteFunction.js   # Шаблон для Appwrite Function
├── .env                      # Переменные окружения
├── index.js                  # Точка входа в приложение
├── netlify.toml              # Настройки Netlify
└── package.json              # Зависимости проекта
```

## Логика работы

1. Сервис проверяет наличие постов с WAV-файлами без соответствующих MP3/HLS
2. Для каждого поста:
   - Загружает WAV файл из Appwrite Storage
   - Конвертирует в MP3
   - Создает HLS сегменты
   - Загружает MP3, сегменты и плейлист в Appwrite Storage
   - Обновляет запись в базе данных с новыми ссылками

## Расширение функциональности

- Для добавления обработки ошибок, модифицируйте файл `src/processor.js`
- Для изменения параметров конвертации аудио, настройте параметры ffmpeg в `src/audioProcessor.js`
- Для поддержки дополнительных форматов, добавьте соответствующие функции в `src/audioProcessor.js`

## Лицензия

MIT 