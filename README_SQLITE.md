# Миграция долговременного состояния LXMF в SQLite

## Цель

Основная цель миграции — ограничить и стабилизировать потребление оперативной
памяти `rsLXMF` на устройствах с 256–512 МБ RAM. Долговременные данные не должны
целиком загружаться в `HashMap`, `Vec` и другие коллекции при запуске. Они должны
храниться в SQLite и извлекаться только перед фактическим использованием.

Скорость доступа к долговременному состоянию вторична: ожидаемая нагрузка на
узел невелика. Приоритетами являются:

- предсказуемый RSS процесса;
- отсутствие монотонного роста памяти при длительной работе;
- атомарность изменений состояния;
- корректное восстановление после перезапуска и внезапного отключения питания;
- сохранение существующего внешнего API настолько, насколько это возможно.

Совместимость формата хранения с Python LXMF не требуется. После миграции
SQLite может быть единственным форматом долговременного хранения.
Импорт существующих `.lxm`-файлов и snapshot-файлов также не требуется:
SQLite-хранилище при первом запуске создаётся с чистого листа.

## Исходное состояние до миграции

До миграции значительная часть состояния загружалась и постоянно удерживалась
в памяти:

- индекс сообщений `PropagationStore`;
- `locally_delivered_ids` и `locally_processed_ids`;
- исходящие сообщения `pending_outbound`;
- сообщения, ожидающие вычисления stamp;
- известные identities;
- полученные ratchets;
- tickets и кэш стоимости stamps.

Два набора transient ID хранят записи до `MESSAGE_EXPIRY * 6`, то есть до
180 дней. На постоянно работающем узле это приводит к длительному накоплению
записей даже при умеренном трафике.

Тела propagation-сообщений находились в отдельных файлах, а их метаданные
дублировались в памяти. Теперь метаданные и payload хранятся в одной записи
базы; параллельные `.lxm`-файлы production daemon не создаёт.

## Реализованная архитектура

SQLite становится источником истины для долговременного состояния. В памяти
остаются только:

- активные сетевые сессии и передачи;
- ограниченные очереди событий;
- сообщение, которое непосредственно обрабатывается или отправляется;
- небольшой ограниченный кэш часто используемых записей, если измерения
  покажут его необходимость;
- активное link/session-состояние и краткоживущее состояние job loop.

Доступ к базе следует скрыть за внутренней абстракцией хранилища:

```text
LxmRouter / PropagationNode / lxmd
                  |
                  v
             LxmfStorage
                  |
                  +-- SqliteStorage
                  +-- MemoryStorage (тесты)
```

### Ошибки хранилища

Открытие базы, проверка схемы и начальная фиксация delivery ratchet/control
state работают fail-closed: daemon не стартует с недоступным или неизвестным
durable state. Операции, после которых наружу возникает необратимое событие,
также fail-closed: announce не передаётся transport до SQLite commit, inbound
hook не запускается до записи сообщения, outbound не считается поставленным
без сохранения нового delivery state.

Производные данные работают fail-open с предупреждением: неудача записи
announce-derived identity/received ratchet или периодического peer checkpoint
не останавливает сетевой цикл, а оставляет последнюю durable версию. Cleanup,
metrics и bounded cache seed при ошибке пропускают текущий проход. Ошибка point
lookup трактуется как cache miss только там, где отсутствие записи уже является
штатным результатом; она не должна подтверждать доставку или удаление данных.

SQLite-соединением должен владеть существующий actor или выделенный storage
actor. Не следует открывать соединение на каждый запрос или предоставлять
нескольким async-задачам прямой доступ через общий `Mutex`.

Блокирующие операции SQLite не должны выполняться на executor-потоке Tokio,
если запрос может читать или удалять много записей. Для редких тяжёлых операций
допускается отдельный blocking worker. Обычные точечные операции могут
последовательно выполняться владельцем хранилища.

## Схема базы

Текущая версия схемы — 7. Версия хранится в `schema_meta`; более новая
неизвестная версия отклоняется при открытии. Хеши хранятся как BLOB с проверкой
длины, timestamps — как Unix time. Ниже показаны основные таблицы сообщений;
полная схема создаётся последовательными migrations в SQLite backend.

```sql
CREATE TABLE schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITHOUT ROWID;

CREATE TABLE messages (
    transient_id       BLOB PRIMARY KEY CHECK(length(transient_id) = 32),
    message_hash       BLOB NOT NULL CHECK(length(message_hash) = 32),
    destination_hash   BLOB NOT NULL CHECK(length(destination_hash) = 16),
    stored_at          INTEGER NOT NULL,
    stamp_value        INTEGER NOT NULL,
    payload            BLOB NOT NULL,
    payload_size       INTEGER NOT NULL,
    collected          INTEGER NOT NULL DEFAULT 0,
    stamped            INTEGER NOT NULL DEFAULT 0
) WITHOUT ROWID;

CREATE INDEX messages_destination
    ON messages(destination_hash, stored_at);

CREATE INDEX messages_stored_at
    ON messages(stored_at);

CREATE TABLE transient_ids (
    transient_id BLOB NOT NULL CHECK(length(transient_id) = 32),
    kind         INTEGER NOT NULL,
    seen_at      INTEGER NOT NULL,
    PRIMARY KEY (kind, transient_id)
) WITHOUT ROWID;

CREATE INDEX transient_ids_seen_at
    ON transient_ids(seen_at);

CREATE TABLE outbound_messages (
    message_id            BLOB PRIMARY KEY CHECK(length(message_id) = 32),
    destination_hash      BLOB NOT NULL CHECK(length(destination_hash) = 16),
    state                 INTEGER NOT NULL,
    delivery_method       INTEGER NOT NULL,
    deferred              INTEGER NOT NULL,
    next_delivery_attempt REAL NOT NULL,
    last_delivery_attempt REAL NOT NULL,
    delivery_attempts     INTEGER NOT NULL,
    created_at            REAL NOT NULL,
    progress              REAL NOT NULL,
    encoded_message       BLOB NOT NULL
) WITHOUT ROWID;

CREATE INDEX outbound_ready
    ON outbound_messages(state, next_delivery_attempt);

CREATE INDEX outbound_deferred_ready
    ON outbound_messages(deferred, next_delivery_attempt);
```

В этой же базе хранятся:

- `identities`;
- `received_ratchets`;
- `tickets`;
- `stamp_costs`;
- `peers`;
- delivery ratchet ring и подписанный announce control state в `state_blobs`;
- принятые сообщения в `inbound_messages`.

`identities` и `received_ratchets` также перенесены в SQLite:
малое количество записей на тестовом запуске было следствием предварительной
очистки каталога и не отражает долговременный production-объём. На сетевом пути
следует использовать ограниченный cache, не загружая таблицы целиком. Активное
состояние link/session принципиально остаётся только в памяти и после restart
создаётся заново.

## Хранение payload

Payload сообщения должен храниться в таблице `messages` как BLOB вместе с
метаданными. Это обеспечивает атомарность: невозможно получить метаданные без
payload или orphan-файл без индексной записи.

При обычных запросах списка, статистики, culling и синхронизации не следует
загружать столбец `payload`. Он читается только перед передачей или выдачей
конкретного сообщения. `payload_size` хранится отдельно для подсчёта размера и
выбора записей при очистке.

Следует проверить поддержку incremental BLOB I/O в выбранной библиотеке. Она
может понадобиться для больших сообщений, чтобы не создавать дополнительную
полную копию payload в памяти. До появления такой необходимости допустимо
загружать один BLOB целиком, если одновременно обрабатывается не более одного
крупного сообщения.

Текущая реализация использует последний вариант: phase-2 read plan хранит
только transient ID, а storage worker загружает BLOB непосредственно перед его
добавлением в ответ. Одновременно загружается не более одного payload, поэтому
incremental BLOB I/O пока не требуется. К этому решению следует вернуться, если
измерения RSS на целевом устройстве покажут неприемлемый пик от одного сообщения.

## Сохранение API

Должны сохраниться внешние интерфейсы, на которых уже построены приложения:

- основные типы сообщений;
- команды и ответы daemon/control protocol;
- router API отправки и обработки сообщений;
- callbacks и события доставки;
- конфигурационные параметры, не относящиеся непосредственно к формату
  хранения.

Внутренние методы, возвращающие ссылки на элементы коллекций, нельзя в общем
случае реализовать поверх SQLite без постоянного RAM-кэша. Например,
`Option<&PropagationEntry>` невозможно безопасно вернуть для только что
прочитанной строки базы. Для таких мест потребуется один из вариантов:

- заменить внутренний метод на возврат owned-значения;
- выполнять действие через callback/closure;
- оставить ограниченный compatibility cache только для реально используемой
  публичной поверхности.

Перед изменением сигнатур необходимо определить, какие методы и публичные поля
используются внешними crates и приложениями. Внутренние изменения не должны
без необходимости становиться breaking change.

## Настройки SQLite

База рассчитана на небольшой одноплатный компьютер и, вероятно, SD-карту.
Фактически применяемые настройки:

```sql
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA temp_store=FILE;
PRAGMA cache_size=-<page_cache_size>;
PRAGMA mmap_size=0;
PRAGMA wal_autocheckpoint=128;
PRAGMA busy_timeout=5000;
PRAGMA auto_vacuum=INCREMENTAL;
```

`page_cache_size` задаётся в конфигурации в КиБ и по умолчанию равен 1024.
Journal mode после открытия проверяется. Эти значения всё ещё необходимо
проверить измерениями RSS и I/O на целевой системе.

Записи с высокой частотой следует группировать в короткие транзакции. Нельзя
выполнять отдельный синхронный commit для каждого принятого пакета или transient
ID: это снижает производительность и ускоряет износ накопителя.

Полный `VACUUM` не должен запускаться автоматически. Для возврата свободных
страниц следует использовать редкий `incremental_vacuum` и контролируемые WAL
checkpoints.

### Backup, restore и выключение

Перед копированием базы daemon следует штатно остановить. При штатной остановке
storage worker завершает принятые операции; периодический PASSIVE checkpoint
не является гарантией пустого WAL. Для согласованной online-копии нужно
использовать SQLite backup API или команду `.backup`, а не копировать только
основной файл работающей базы. Для offline backup после остановки следует
сохранить `lxmf.sqlite` вместе с существующими `lxmf.sqlite-wal` и
`lxmf.sqlite-shm` либо предварительно выполнить checkpoint штатным SQLite
инструментом.

Для восстановления daemon должен быть остановлен. Нужно заменить согласованный
комплект файлов базы и сохранить права доступа владельца; на Unix daemon
устанавливает режим `0600`. Identity находится вне `storage` и в backup базы не
входит. Удаление каталога `storage` означает намеренный запуск с пустым durable
state; импорт старых файловых форматов не выполняется.

## Надёжность и ограничения размера

Операции должны быть транзакционными:

- вставка сообщения и регистрация transient ID;
- удаление сообщения и обновление статистики;
- изменение состояния outbound message;
- пакетная очистка просроченных записей.

Ограничение message store должно учитывать:

- сумму `payload_size`;
- временный размер WAL;
- служебные страницы и индексы;
- запас свободного места файловой системы.

Weighted culling можно выполнять пакетами. SQL выбирает кандидатов без чтения
payload, Rust вычисляет или уточняет вес, после чего выбранные строки удаляются
одной транзакцией.

## Наблюдаемость и измерения

Для проверки результата миграции полезны периодические диагностические метрики:

- RSS и high-water mark процесса;
- количество и суммарный payload `pending_outbound`;
- количество `pending_deferred_stamps`;
- количество и размер propagation messages;
- количество locally delivered/processed transient IDs;
- количество known identities и received ratchets;
- размер peer distribution queue;
- размер файла БД и WAL;
- число операций чтения, записи и промахов RAM-кэша;
- длительность culling и checkpoint.

Эти показатели нужны, чтобы отличить рост долговременного состояния от
удержания буферов, незавершённых transfers или фрагментации allocator.

`[storage] vacuum_interval` задаёт период PASSIVE WAL checkpoint и
incremental vacuum (не менее 60 секунд), а `vacuum_pages` ограничивает число
страниц за один проход. Maintenance выполняется storage worker’ом и
откладывается при активной propagation sync-сессии. Полный `VACUUM`
автоматически не запускается.

`[storage] database_path` переопределяет стандартный путь
`<config-dir>/storage/lxmf/lxmf.sqlite`. Абсолютное значение используется как
есть; относительное разрешается от LXMF config directory. Родительский каталог
создаётся daemon’ом. Перенос уже существующей базы при смене параметра
автоматически не выполняется.

`[storage] page_cache_size` задаёт бюджет SQLite page cache в КиБ через
отрицательное значение `PRAGMA cache_size`. Значение ограничивается диапазоном
64–65536 КиБ; по умолчанию используется 1024 КиБ. Это верхняя целевая величина
кэша SQLite, а не жёсткий лимит RSS всего процесса.

Существующий `[propagation] message_storage_limit` используется также как
физический бюджет DB+WAL. При превышении удаляются только propagation messages
по существующей weighted policy; outbound, inbound, identities, tickets и
crypto state автоматически не удаляются. Если база без propagation payload
сама превышает лимит, daemon сообщает об этом, но сохраняет durable state.

## Этапы реализации

1. Добавить метрики и подтвердить источники роста.
2. Ввести внутреннюю абстракцию хранилища.
3. Перенести `locally_delivered_ids` и `locally_processed_ids`.
4. Перенести propagation metadata и payload в одну таблицу.
5. Удалить сканирование каталога сообщений и параллельные `.lxm`-файлы.
6. Перенести `pending_outbound` и deferred messages.
7. Перенести identities, ratchets, tickets, stamp costs и peer metadata.
8. Перенести delivery ratchet/control state и удалить production-вызовы
   устаревшего файлового persistence.

На каждом этапе должны сохраняться функциональные тесты и добавляться проверки
восстановления после перезапуска, аварийного прерывания транзакции и достижения
лимита хранилища.
