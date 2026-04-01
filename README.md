# Embeddable ETL

Небольшой сервис на Go, который предоставляет HTTP API для простых ETL‑задач. Он сохраняет входящие строки в Postgres, ведёт статистику запусков и складывает «плохие» записи в dead‑letter таблицу. Проект делал как учебный, но максимально приближал к «боевому» стилю: миграции, Docker, аккуратные ручки и минимальная схема пайплайнов.

## Сильные стороны проекта
- **Практичность**: запуск ETL через HTTP и стабильные метрики по запускам
- **Прозрачный поток данных**: хорошие строки — в `etl_rows`, плохие — в `etl_dead_letters`
- **Самодостаточность**: миграции + Docker Compose позволяют быстро подняться
- **Расширяемость**: схемы пайплайнов хранятся в БД и легко развиваются

### Как используются технологии
- **Go** — основной язык, HTTP сервер на `net/http`, обработчики и валидация данных.
- **PostgreSQL** — хранение run‑ов, строк, dead‑letters и схем пайплайнов.
- **pgx** — пул соединений и доступ к БД.
- **Docker / Compose** — быстрое поднятие сервиса и Postgres локально.
- **Миграции** — SQL‑файлы в `migrations/`, применяются при старте.
- **CLI** — отдельный бинарник для ручной работы с API.
- **Тесты** — end‑to‑end на реальной БД с проверкой run‑статистики.

## Возможности
- Эндпоинты health/ready
- Runs API со статусом и счётчиками
- Ингест JSON‑строки + dead‑letters
- Ингест CSV
- Пайплайны со схемной валидацией
- Мини‑CLI для работы с API

## Алгоритм обработки ingestion при помощи схемы
1. Клиент создаёт pipeline со схемой (`/v1/pipelines`).
2. CSV отправляется в `/v1/pipelines/ingest/csv?pipeline_id=...`.
3. Сервер читает header и валидирует строки по схеме.
4. Валидные строки сохраняются в `etl_rows`.
5. Ошибочные строки пишутся в `etl_dead_letters` вместе с причиной.
6. В таблице `runs` обновляется статистика: total/ok/bad.

## Структура проекта
```
cmd/
  server/   # HTTP API service
  CLI/      # small CLI client
migrations/ # SQL migrations
Dockerfile
docker-compose.yml
```

## Быстрый старт (Docker)
1. Создай `.env` с `ETL_DB_DSN` (формат см. в `docker-compose.yml`).
2. Запусти стек:

```bash
docker compose up --build
```

API будет доступен на `http://localhost:8080`.

## Обзор API
### Health
- `GET /dev/healthz`
- `GET /dev/readyz`

### Runs
- `POST /dev/runs` → создать run
- `GET /dev/runs?id=...` → получить run по id

### Rows
- `POST /dev/rows?run_id=...` → вставить JSON‑payload

### Pipelines
- `POST /v1/pipelines` → создать pipeline со схемой
- `GET /v1/pipelines?pipeline_id=...` → получить pipeline

### CSV ingestion
- `POST /v1/ingest/csv?run_id=...` → общий CSV ingest
- `POST /v1/pipelines/ingest/csv?pipeline_id=...` → CSV ingest со схемной валидацией

## Пример схемы
```json
{
  "fields": [
    {"name": "email", "type": "string", "required": true, "format": "email"},
    {"name": "age", "type": "int", "required": true},
    {"name": "note", "type": "string", "required": false}
  ]
}
```

Готовые примеры лежат в папке `examples/`:
- `examples/schema.json`
- `examples/data.csv`

## Использование CLI
CLI находится в `cmd/CLI` и обращается к HTTP API. Базовый URL задаётся через `ETL_URL`.

### Типичные команды
- Проверка состояния сервиса
- Создание pipeline
- Ингест CSV по pipeline

```bash
ETL_URL=http://localhost:8080 go run ./cmd/CLI health
ETL_URL=http://localhost:8080 go run ./cmd/CLI pipeline-create -name users -schema ./schema.json
ETL_URL=http://localhost:8080 go run ./cmd/CLI pipeline-ingest-csv -pipeline-id 1 -csv ./data.csv
```

## Тесты
End‑to‑end тесты используют реальный Postgres. Они идемпотентны и могут запускаться много раз.

```bash
go test ./cmd/server -run TestPipelineIngestCSV
```

## Чему я научился
- Проектировать небольшой, но цельный сервис с понятным API
- Работать с миграциями и поддерживать схему БД в актуальном состоянии
- Обрабатывать ошибки и сохранять проблемные записи в отдельную таблицу
- Делать идемпотентные end‑to‑end тесты на живой БД
- Организовывать проектную структуру и выделять отдельный CLI


## Дальнейшие шаги
- Добавить более богатые правила валидации (min/max, enum и т.д.)
- Улучшить ответы ошибок и документацию API
- Добавить CI пайплайн для тестов

---
