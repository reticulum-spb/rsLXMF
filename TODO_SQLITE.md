# План работ по миграции rsLXMF на SQLite

## 0. Подготовка и измерения

- [ ] Зафиксировать baseline RSS на целевом OrangePi после запуска.
- [ ] Зафиксировать RSS и размеры коллекций через 1, 6, 24 и 72 часа.
- [ ] Добавить периодические счётчики:
  - [ ] `pending_outbound`: количество и приблизительный размер payload;
  - [ ] `pending_deferred_stamps`: количество и размер;
  - [ ] propagation messages: количество и суммарный размер;
  - [ ] `locally_delivered_ids`;
  - [ ] `locally_processed_ids`;
  - [ ] `known_identities`;
  - [ ] `received_ratchets`;
  - [ ] peer distribution queue;
  - [ ] входные каналы и активные resource transfers.
- [ ] Проверить, освобождается ли RSS после принудительной очистки коллекций.
- [ ] Отделить постоянный рост состояния от фрагментации allocator.
- [ ] Описать целевые ограничения памяти:
  - [ ] нормальный RSS для узла с 256 МБ RAM;
  - [ ] допустимый кратковременный пик;
  - [ ] минимальный резерв `MemAvailable`.

## 1. Аудит совместимости API

- [x] Составить список публичных типов и методов `lxmf-core`.
- [x] Найти публичные поля с `HashMap`, `Vec` или `PropagationStore`.
- [x] Найти методы, возвращающие `&T`, `&mut T` и итераторы по ссылкам.
- [x] Проверить использование этих API в `lxmf-tools` и `examples`.
- [x] Сопоставить API с уже существующими внешними приложениями.
- [x] Определить список сигнатур, которые обязательно должны сохраниться.
- [ ] Для несовместимых с SQLite методов выбрать:
  - [x] owned return для внутренних point lookup;
  - [ ] callback/closure API, если потребуется streaming;
  - [x] ограниченный compatibility cache для старого reference API;
  - [x] deprecated wrapper для full-map/file API.
- [ ] Добавить compile-time/API regression tests для сохраняемых интерфейсов.

Результаты аудита: `doc/api.md`.

## 2. Рефакторинг daemon на high-level API

Этот этап выполняется до подключения SQLite. `lxmf-tools` не должен напрямую
читать или изменять коллекции `LxmRouter`, которые позднее перестанут быть
полным представлением состояния в RAM.

- [ ] Составить полный список прямых обращений `lxmf-tools` к публичным полям
  `LxmRouter`.
- [ ] Составить полный список прямых обращений daemon к:
  - [ ] `pending_outbound`;
  - [ ] `pending_deferred_stamps`;
  - [ ] `propagation_store`;
  - [ ] `outbound_stamp_costs`;
  - [ ] `ticket_store`;
  - [ ] `peers`;
  - [ ] policy maps/lists;
  - [ ] throttled peers;
  - [ ] внутренним propagation counters/state.
- [ ] Добавить high-level command methods:
  - [ ] enqueue/cancel/update outbound message;
  - [ ] enqueue/cancel/query deferred stamp;
  - [ ] mark delivery result;
  - [ ] add/update/remove peer;
  - [ ] add/remove policy entry;
  - [ ] add/remove throttle;
  - [ ] remember/remove ticket и stamp cost.
- [ ] Добавить high-level read/query methods:
  - [ ] outbound count и bounded summary;
  - [ ] propagation count/total size;
  - [ ] point lookup propagation metadata;
  - [ ] paginated propagation metadata;
  - [ ] peer summaries/stats;
  - [ ] ticket/stamp-cost lookup;
  - [ ] router/node status snapshot.
- [ ] Не возвращать из новых методов ссылки на внутренние `HashMap`/`Vec`.
- [ ] Для списков возвращать owned DTO и использовать обязательный `limit`.
- [ ] Для больших payload отделить metadata query от payload read.
- [ ] Добавить отдельные DTO для daemon/control status вместо клонирования
  внутренних router types.
- [ ] Перевести `lxmf-tools` на новые high-level methods.
- [ ] Перевести control handlers на status/query DTO.
- [ ] Перевести peer management и sync scheduling на router methods.
- [ ] Перевести сохранение tickets/stamp costs/transient IDs на storage API.
- [ ] Перевести daemon tests на high-level API.
- [ ] Убедиться, что production-код `lxmf-tools` больше не обращается напрямую
  к storage-sensitive полям `LxmRouter`.
- [ ] Добавить проверку/тест, запрещающий новые прямые обращения к этим полям.
- [ ] Сохранить старые публичные поля на переходный период для source
  compatibility, но перестать использовать их как источник истины.
- [ ] Пометить representation-leaking поля deprecated после перевода всех
  внутренних consumers.
- [ ] Проверить неизменность daemon CLI и control protocol.
- [ ] Выполнить regression tests `lxmf-core`, `lxmf-tools` и examples.

## 3. Storage abstraction и выбор SQLite-библиотеки

- [ ] Выбрать SQLite-библиотеку (`rusqlite` как основной кандидат).
- [ ] Решить, использовать системную SQLite или bundled feature.
- [ ] Оценить размер итогового бинарника для обеих конфигураций.
- [ ] Добавить crate feature для SQLite backend, если нужен переходный режим.
- [ ] Ввести общий тип `StorageError`.
- [ ] Определить trait или внутренний интерфейс `LxmfStorage`, не содержащий
  SQLite-specific types.
- [ ] Разделить storage API по назначению:
  - [ ] transient ID operations;
  - [ ] propagation metadata operations;
  - [ ] propagation payload operations;
  - [ ] outbound queue operations;
  - [ ] identity/ratchet/ticket/stamp-cost operations.
- [ ] Для каждой группы определить point, bounded-page и batch operations.
- [ ] Не добавлять в storage trait методы, возвращающие `&T`, `&mut T`,
  `&HashMap` или `&[T]`.
- [ ] Не добавлять обязательную загрузку всей таблицы в `Vec`.
- [ ] Определить транзакционные границы между metadata, payload и transient ID.
- [ ] Реализовать `MemoryStorage` для быстрых unit-тестов.
- [ ] Перевести `LxmRouter` и `PropagationNode` на storage abstraction с
  `MemoryStorage`, не меняя поведение.
- [ ] Передавать storage через constructor/builder, не через глобальный
  singleton.
- [ ] Определить ownership storage между `LxmRouter`, `PropagationNode` и
  daemon, исключив дублирующие соединения и кэши.
- [ ] Реализовать открытие и проверку SQLite database.
- [ ] Добавить версию схемы и механизм последовательных migrations.
- [ ] Запретить молчаливое открытие базы с более новой неизвестной схемой.
- [ ] Определить владельца соединения:
  - [ ] `LxmRouter`/`PropagationNode` actor;
  - [ ] либо отдельный storage actor.
- [ ] Исключить выполнение длительных SQL-операций на Tokio executor thread.
- [ ] Добавить mock/failure injection для storage errors.
- [ ] Определить fail-open/fail-closed поведение для каждой операции.
- [ ] Добавить contract tests, одинаковые для `MemoryStorage` и
  `SqliteStorage`.

## 4. Конфигурация базы

- [ ] Добавить конфигурационный путь к SQLite database.
- [ ] Добавить конфигурируемый предел SQLite page cache.
- [ ] Установить и протестировать:
  - [ ] `journal_mode=WAL`;
  - [ ] `synchronous=NORMAL`;
  - [ ] `temp_store=FILE`;
  - [ ] `cache_size`;
  - [ ] `mmap_size=0`;
  - [ ] `wal_autocheckpoint`;
  - [ ] `busy_timeout`;
  - [ ] `auto_vacuum=INCREMENTAL`.
- [ ] Проверять фактически применённый journal mode.
- [ ] Установить безопасные filesystem permissions для файла базы, WAL и SHM.
- [ ] Определить поведение при read-only filesystem.
- [ ] Определить поведение при отсутствии места на диске.
- [ ] Определить политику WAL checkpoint.
- [ ] Не запускать автоматический полный `VACUUM`.

## 5. Transient ID store — первый функциональный этап

- [ ] Создать таблицу `transient_ids`.
- [ ] Закодировать типы locally delivered и locally processed.
- [ ] Реализовать point lookup по `(kind, transient_id)`.
- [ ] Реализовать `INSERT ... ON CONFLICT`.
- [ ] Реализовать пакетную запись transient IDs.
- [ ] Реализовать очистку записей старше `MESSAGE_EXPIRY * 6`.
- [ ] Удалить загрузку обеих таблиц целиком в память при старте.
- [ ] Перевести `is_locally_delivered`.
- [ ] Перевести `is_locally_processed`.
- [ ] Перевести `mark_locally_delivered`.
- [ ] Перевести `mark_locally_processed`.
- [ ] Удалить старое snapshot-сохранение transient ID.
- [ ] Добавить тесты сохранения между перезапусками.
- [ ] Добавить тесты expiry boundary.
- [ ] Добавить тесты повторной вставки ID.
- [ ] Измерить RSS до и после этапа.

## 6. Propagation messages: metadata и payload

- [ ] Создать таблицу `messages`.
- [ ] Хранить metadata и payload одной атомарной записью.
- [ ] Проверять длины transient ID, message hash и destination hash.
- [ ] Реализовать вставку сообщения.
- [ ] Реализовать point lookup без чтения payload.
- [ ] Реализовать отдельную загрузку payload.
- [ ] Реализовать проверку существования сообщения.
- [ ] Реализовать выборку IDs.
- [ ] Реализовать выборку сообщений по destination.
- [ ] Реализовать удаление сообщения.
- [ ] Реализовать обновление `collected`.
- [ ] Реализовать получение `stamp_value`.
- [ ] Реализовать `COUNT(*)` и `SUM(payload_size)`.
- [ ] Реализовать expiry culling без загрузки payload.
- [ ] Реализовать weighted culling пакетами.
- [ ] Учесть prioritised destinations при weighted culling.
- [ ] Сделать удаление выбранных кандидатов одной транзакцией.
- [ ] Перевести peer distribution на хранение только transient IDs.
- [ ] Не загружать payload при составлении offers и статистики.
- [ ] Проверить необходимость incremental BLOB I/O.
- [ ] Ограничить число одновременно загруженных крупных payload.
- [ ] Добавить тесты storage limit.
- [ ] Добавить тесты weighted culling.
- [ ] Добавить тесты сообщений с максимальным payload.
- [ ] Добавить тест восстановления после прерывания вставки.

## 7. Отказ от отдельных message-файлов

- [ ] Удалить создание новых `.lxm`-файлов после включения SQLite backend.
- [ ] Удалить сканирование messages directory при запуске.
- [ ] Удалить построение RAM-индекса из имён файлов.
- [ ] Удалить синхронизацию metadata и файлов.
- [ ] Удалить orphan-file cleanup.
- [ ] Решить, нужна ли одноразовая миграция уже существующих файлов:
  - [ ] реализовать импорт;
  - [ ] либо документировать, что старое хранилище начинается заново.
- [ ] После переходного периода удалить устаревшие persistence-функции.
- [ ] Обновить конфигурацию и документацию путей хранения.

## 8. Outbound и deferred messages

- [ ] Создать таблицу `outbound_messages`.
- [ ] Определить стабильное кодирование полного `LxMessage`.
- [ ] Хранить delivery state отдельно от encoded message.
- [ ] Индексировать `state` и `next_delivery_attempt`.
- [ ] Перенести `pending_outbound` в SQLite.
- [ ] Выбирать только готовые к попытке сообщения.
- [ ] Загружать полное сообщение непосредственно перед обработкой.
- [ ] После попытки атомарно обновлять attempts, progress и next attempt.
- [ ] Удалять успешно доставленные и окончательно failed сообщения.
- [ ] Перенести `pending_deferred_stamps`.
- [ ] В памяти оставлять только одну активную stamp job.
- [ ] Корректно восстанавливать незавершённую stamp job после restart.
- [ ] Сохранить порядок и семантику callbacks.
- [ ] Добавить тесты повторных попыток после restart.
- [ ] Добавить тесты expiry `MESSAGE_EXPIRY`.
- [ ] Добавить тесты `MAX_DELIVERY_ATTEMPTS`.
- [ ] Измерить RSS при искусственно большой outbound queue.

## 9. Остальное долговременное состояние

- [ ] Измерить вклад `known_identities`.
- [ ] При необходимости перенести identities в SQLite.
- [ ] Добавить ограниченный LRU identities с настраиваемым размером.
- [ ] Измерить вклад `received_ratchets`.
- [ ] При необходимости перенести received ratchets.
- [ ] Гарантировать атомарную замену ratchet.
- [ ] Перенести tickets.
- [ ] Перенести outbound stamp costs.
- [ ] Проверить необходимость переноса peer metadata.
- [ ] Не переносить активное состояние link/session в SQLite.

## 10. Ограничение дискового пространства и обслуживание

- [ ] Учитывать размер payload отдельно от размера файла БД.
- [ ] Контролировать размер основного файла, WAL и свободное место.
- [ ] Оставлять резерв для checkpoint и транзакций.
- [ ] Определить поведение при достижении storage limit.
- [ ] Определить порядок удаления сообщений при нехватке места.
- [ ] Реализовать ограниченный incremental vacuum.
- [ ] Не выполнять vacuum во время активной resource transfer.
- [ ] Добавить maintenance metrics:
  - [ ] database size;
  - [ ] WAL size;
  - [ ] free pages;
  - [ ] last checkpoint duration;
  - [ ] last cull duration.
- [ ] Проверить износ и объём записи на SD-карту.

## 11. Тестирование надёжности

- [ ] Unit-тесты всех CRUD-операций.
- [ ] Тесты schema migrations.
- [ ] Тест открытия повреждённой базы.
- [ ] Тесты неожиданного завершения процесса в середине транзакции.
- [ ] Тесты восстановления после незавершённого WAL checkpoint.
- [ ] Тесты заполненного диска.
- [ ] Тесты read-only filesystem.
- [ ] Тесты одновременных control-запросов и propagation sync.
- [ ] Тесты больших payload на 256 МБ RAM.
- [ ] Длительный soak test не менее 72 часов.
- [ ] Длительный тест с TCP, LoRa и KISS интерфейсами.
- [ ] Сравнить RSS, high-water mark и число записей с baseline.
- [ ] Проверить отсутствие монотонного роста RSS при стабильном числе записей.

## 12. Документация и завершение

- [ ] Описать новый путь и формат базы.
- [ ] Описать параметры ограничения памяти и page cache.
- [ ] Описать backup/restore SQLite database.
- [ ] Описать безопасное выключение и checkpoint.
- [ ] Описать последствия удаления старой Python/file-format совместимости.
- [ ] Обновить основной README после стабилизации backend.
- [ ] Удалить неиспользуемый код старого persistence.
- [ ] Удалить переходные feature flags, если они больше не нужны.
- [ ] Провести финальный аудит публичного API.
- [ ] Зафиксировать результаты измерений на OrangePi 256 МБ и 512 МБ.
