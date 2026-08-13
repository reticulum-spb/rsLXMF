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
- [x] Для несовместимых с SQLite методов выбрать:
  - [x] owned return для внутренних point lookup;
  - [x] callback/closure API пока не требуется; добавить при появлении streaming;
  - [x] ограниченный compatibility cache для старого reference API;
  - [x] deprecated wrapper для full-map/file API.
- [x] Добавить compile-time/API regression tests для сохраняемых интерфейсов.

Результаты аудита: `doc/api.md`.

## 2. Рефакторинг daemon на high-level API

Этот этап выполняется до подключения SQLite. `lxmf-tools` не должен напрямую
читать или изменять коллекции `LxmRouter`, которые позднее перестанут быть
полным представлением состояния в RAM.

- [x] Составить полный список прямых обращений `lxmf-tools` к публичным полям
  `LxmRouter`.
- [x] Составить полный список прямых обращений daemon к:
  - [x] `pending_outbound`;
  - [x] `pending_deferred_stamps`;
  - [x] `propagation_store`;
  - [x] `outbound_stamp_costs`;
  - [x] `ticket_store`;
  - [x] `peers`;
  - [x] policy maps/lists;
  - [x] throttled peers;
  - [x] внутренним propagation counters/state.
- [x] Добавить high-level command methods:
  - [x] enqueue/cancel/update outbound message;
  - [x] enqueue/cancel/query deferred stamp;
  - [x] mark delivery result;
  - [x] add/update/remove peer;
  - [x] add/remove policy entry;
  - [x] add/remove throttle;
  - [x] remember/remove ticket и stamp cost.
- [x] Добавить high-level read/query methods:
  - [x] outbound count и bounded summary;
  - [x] propagation count/total size;
  - [x] point lookup propagation metadata;
  - [x] paginated propagation metadata;
  - [x] peer summaries/stats;
  - [x] ticket/stamp-cost lookup;
  - [x] router/node status snapshot.
- [x] Не возвращать из новых методов ссылки на внутренние `HashMap`/`Vec`.
- [x] Для списков возвращать owned DTO и использовать обязательный `limit`.
- [x] Для больших payload отделить metadata query от payload read.
- [x] Добавить отдельные DTO для daemon/control status вместо клонирования
  внутренних router types.
- [x] Перевести `lxmf-tools` на новые high-level methods.
- [x] Перевести control handlers на status/query DTO.
- [x] Перевести peer management и sync scheduling на router methods.
- [x] Перевести сохранение tickets/stamp costs/transient IDs на storage API.
- [x] Перевести daemon tests на high-level API.
- [x] Убедиться, что production-код `lxmf-tools` больше не обращается напрямую
  к storage-sensitive полям `LxmRouter`.
- [x] Добавить проверку/тест, запрещающий новые прямые обращения к этим полям.
- [x] Сохранить старые публичные поля на переходный период для source
  compatibility, но перестать использовать их как источник истины.
- [x] Пометить representation-leaking поля deprecated после перевода всех
  внутренних consumers.
- [x] Проверить неизменность daemon CLI и control protocol.
- [ ] Выполнить regression tests `lxmf-core`, `lxmf-tools` и examples.

## 3. Storage abstraction и выбор SQLite-библиотеки

- [x] Выбрать SQLite-библиотеку (`rusqlite`).
- [x] Использовать bundled SQLite по умолчанию; оставить отключаемый `sqlite`
  feature для системной библиотеки.
- [ ] Оценить размер итогового бинарника для обеих конфигураций.
- [x] Добавить crate features `sqlite` и `sqlite-bundled`.
- [x] Ввести общий тип `StorageError`.
- [x] Определить trait или внутренний интерфейс `LxmfStorage`, не содержащий
  SQLite-specific types.
- [ ] Разделить storage API по назначению:
  - [x] transient ID operations;
  - [x] propagation metadata operations;
  - [x] propagation payload operations;
  - [x] outbound queue operations;
  - [x] identity/ratchet/ticket/stamp-cost operations.
- [ ] Для каждой группы определить point, bounded-page и batch operations.
- [x] Не добавлять в storage trait методы, возвращающие `&T`, `&mut T`,
  `&HashMap` или `&[T]`.
- [x] Не добавлять обязательную загрузку всей таблицы в `Vec`.
- [x] Хранить metadata и payload одной атомарной storage operation;
  transient-ID batch выполняется отдельной транзакцией.
- [x] Реализовать `MemoryStorage` для быстрых unit-тестов.
- [x] Перевести `LxmRouter` и production `PropagationNode` на storage abstraction с
  `MemoryStorage`, не меняя поведение.
- [x] Передавать storage через constructor/builder, не через глобальный
  singleton.
- [x] Определить ownership storage между `LxmRouter`, `PropagationNode` и
  daemon, исключив дублирующие соединения и кэши.
- [x] Реализовать открытие и проверку SQLite database.
- [x] Добавить версию схемы и механизм последовательных migrations.
- [x] Запретить молчаливое открытие базы с более новой неизвестной схемой.
- [x] Определить владельца соединения:
  - [ ] `LxmRouter`/`PropagationNode` actor;
  - [x] отдельный blocking storage worker с клонируемым handle.
- [x] Исключить выполнение SQL-операций на Tokio executor thread.
- [x] Добавить детерминированный failure injection на storage actor handle для
  проверки ошибок без остановки worker или подмены backend.
- [x] Определить fail-open/fail-closed поведение по классам операций и
  задокументировать необратимые события, cache/derived state и maintenance.
- [x] Добавить contract tests, одинаковые для `MemoryStorage` и
  `SqliteStorage`.

## 4. Конфигурация базы

- [x] Добавить `[storage] database_path` с абсолютным путём или разрешением
  относительного пути от LXMF config directory.
- [ ] Добавить конфигурируемый предел SQLite page cache.
- [x] Установить и протестировать:
  - [x] `journal_mode=WAL`;
  - [x] `synchronous=NORMAL`;
  - [x] `temp_store=FILE`;
  - [x] `cache_size`;
  - [x] `mmap_size=0`;
  - [x] `wal_autocheckpoint`;
  - [x] `busy_timeout`;
  - [x] `auto_vacuum=INCREMENTAL`.
- [x] Проверять фактически применённый journal mode.
- [x] Установить безопасные filesystem permissions `0600` для файла базы, WAL и SHM на Unix.
- [ ] Определить поведение при read-only filesystem.
- [ ] Определить поведение при отсутствии места на диске.
- [x] Определить политику WAL checkpoint: периодический PASSIVE checkpoint на storage worker.
- [x] Не запускать автоматический полный `VACUUM`.

## 5. Transient ID store — первый функциональный этап

- [x] Создать таблицу `transient_ids`.
- [x] Закодировать типы locally delivered и locally processed.
- [x] Реализовать point lookup по `(kind, transient_id)`.
- [x] Реализовать `INSERT ... ON CONFLICT`.
- [x] Реализовать пакетную запись transient IDs.
- [x] Реализовать очистку записей по переданной expiry boundary.
- [x] Удалить загрузку обеих таблиц целиком в память при старте.
- [x] Перевести `is_locally_delivered`.
- [x] Перевести `is_locally_processed`.
- [x] Перевести `mark_locally_delivered`.
- [x] Перевести `mark_locally_processed`.
- [x] Удалить старое snapshot-сохранение transient ID.
- [x] Добавить тесты сохранения между перезапусками.
- [x] Добавить тесты expiry boundary.
- [x] Добавить тесты повторной вставки ID.
- [ ] Измерить RSS до и после этапа.

## 6. Propagation messages: metadata и payload

- [x] Создать таблицу `messages`.
- [x] Хранить metadata и payload одной атомарной записью.
- [x] Проверять длины transient ID, message hash и destination hash.
- [x] Реализовать вставку сообщения.
- [x] Реализовать point lookup без чтения payload.
- [x] Реализовать отдельную загрузку payload.
- [x] Реализовать проверку существования сообщения.
- [x] Реализовать bounded выборку IDs/metadata.
- [x] Реализовать выборку сообщений по destination.
- [x] Реализовать удаление сообщения.
- [x] Реализовать обновление `collected`.
- [x] Реализовать получение `stamp_value` через metadata.
- [x] Реализовать `COUNT(*)` и `SUM(payload_size)`.
- [x] Реализовать expiry culling пакетами без загрузки payload.
- [x] Реализовать weighted culling пакетами.
- [x] Учесть prioritised destinations при weighted culling.
- [x] Сделать удаление выбранных кандидатов одной транзакцией.
- [x] Перевести peer distribution на хранение только transient IDs.
- [x] Не загружать payload при составлении offers и статистики.
- [x] Проверить необходимость incremental BLOB I/O: пока не требуется, решение
  описано в `README_SQLITE.md`.
- [x] Ограничить число одновременно загруженных крупных payload.
- [x] Добавить тесты storage limit.
- [x] Добавить тесты weighted culling.
- [x] Добавить тесты сообщений с максимальным payload.
- [x] Добавить тест восстановления после прерывания вставки.

## 7. Отказ от отдельных message-файлов

- [x] Не создавать новые `.lxm`-файлы в production SQLite backend.
- [x] Не сканировать messages directory при SQLite startup.
- [x] Не строить RAM-индекс из имён файлов в SQLite backend.
- [x] Не синхронизировать SQLite metadata с файлами.
- [x] Не запускать orphan-file cleanup в SQLite backend.
- [x] Одноразовую миграцию существующих файлов не реализовывать: SQLite
  storage всегда создаётся с чистого листа.
- [x] Удалить устаревшие router persistence-функции; файловый backend
  `PropagationNode` оставить deprecated только для публичной API-совместимости.
- [x] Обновить конфигурацию и документацию путей хранения.

## 8. Outbound и deferred messages

- [x] Создать таблицу `outbound_messages`.
- [x] Определить стабильное версионированное кодирование полного `LxMessage`.
- [x] Хранить delivery state отдельно от encoded message.
- [x] Индексировать `state` и `next_delivery_attempt`.
- [x] Перенести `pending_outbound` в SQLite.
- [x] Выбирать только готовые к попытке сообщения.
- [x] Загружать полное сообщение непосредственно перед обработкой.
- [x] После попытки атомарно обновлять attempts, progress и next attempt.
- [x] Удалять успешно доставленные и окончательно failed сообщения.
- [x] Перенести `pending_deferred_stamps`.
- [x] В памяти оставлять только одну активную stamp job.
- [x] Корректно восстанавливать незавершённую stamp job после restart.
- [x] Сохранить порядок и семантику callbacks через runtime callback cache.
- [x] Добавить тесты повторных попыток после restart.
- [x] Добавить тесты expiry `MESSAGE_EXPIRY`.
- [x] Добавить тесты `MAX_DELIVERY_ATTEMPTS`.
- [x] Добавить bounded-memory тест искусственно большой outbound queue; реальное
  измерение RSS на OrangePi остаётся в разделе 0.

## 9. Остальное долговременное состояние

- [ ] Измерить вклад `known_identities` на накопленном production-состоянии.
- [x] Перенести identities в SQLite.
- [x] Добавить ограниченный hot cache identities/ratchets (512 записей);
  конфигурируемый размер вынести в конфигурацию на этапе настройки памяти.
- [ ] Измерить вклад `received_ratchets` на накопленном production-состоянии.
- [x] Перенести received ratchets в SQLite.
- [x] Гарантировать атомарную замену ratchet в SQLite через UPSERT.
- [x] Перенести tickets.
- [x] Перенести outbound stamp costs.
- [x] Перенести peer metadata и handled-message sets в SQLite; восстанавливать
  при старте и атомарно чекпоинтить bounded-набор peers.
- [x] Перенести delivery ratchet ring и подписанный announce control state в
  SQLite blobs; фиксировать оба документа одной транзакцией до отправки announce.
- [x] Прекратить создание `storage/messages`, `storage/lxmf/ratchets` и вызовы
  устаревших `load_state`/`save_state` в production `lxmd`.
- [x] Не переносить активное состояние link/session в SQLite.

## 10. Ограничение дискового пространства и обслуживание

- [ ] Учитывать размер payload отдельно от размера файла БД.
- [x] Контролировать размер основного файла, WAL и SQLite freelist.
- [ ] Оставлять резерв для checkpoint и транзакций.
- [x] Определить поведение при достижении storage limit: удалять только
  propagation payload, не затрагивая durable outbound/inbound/crypto state.
- [x] Определить порядок удаления сообщений через существующую weighted policy.
- [x] Реализовать ограниченный incremental vacuum.
- [x] Не выполнять vacuum во время активной propagation sync/resource transfer.
- [ ] Добавить maintenance metrics:
  - [x] database size;
  - [x] WAL size;
  - [x] free pages;
  - [x] last checkpoint/maintenance duration;
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

- [x] Описать новый путь и формат базы.
- [ ] Описать параметры ограничения памяти и page cache.
- [ ] Описать backup/restore SQLite database.
- [ ] Описать безопасное выключение и checkpoint.
- [ ] Описать последствия удаления старой Python/file-format совместимости.
- [ ] Обновить основной README после стабилизации backend.
- [x] Удалить неиспользуемый код старого router persistence.
- [ ] Удалить переходные feature flags, если они больше не нужны.
- [ ] Провести финальный аудит публичного API.
- [ ] Зафиксировать результаты измерений на OrangePi 256 МБ и 512 МБ.
