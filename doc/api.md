# Публичный API rsLXMF

## Назначение

Документ фиксирует публичную поверхность `rsLXMF` перед переносом
долговременного состояния в SQLite. Он отделяет совместимый прикладной и
протокольный API от текущего представления данных в `HashMap`, `Vec` и
отдельных файлах.

Аудит выполнен по:

- `lxmf-core`;
- `lxmf-tools`;
- examples и tests workspace `rsLXMF`;
- всем проектам текущей рабочей папки:
  `rsReticulum`, `rsNodePage`, `rsNomadNet`, `rsRRC`, `rsRRC-client`,
  `rsRRCD`.

Из внешних проектов напрямую от `lxmf-core` зависит только `rsNomadNet`.
`lxmf-tools`, examples и tests являются широкими внутренними потребителями и
также входят в compatibility contract.

## Итог аудита

`rsNomadNet` использует:

- `DeliveryIdentity`;
- `DELIVERY_APP_NAME`;
- `DeliveryMethod`;
- `UnverifiedReason`;
- `LxMessage` и `MessageError`;
- `PropagationClient` и `PropagationClientState`;
- парсеры delivery/propagation announce app data.

Он не обращается напрямую к:

- `LxmRouter`;
- `PropagationStore`;
- `PropagationNode`;
- `TicketStore`;
- модулю `persist`;
- внутренним message directories;
- peer/session maps.

Следовательно, SQLite storage можно скрыть внутри `LxmRouter`,
`PropagationNode` и daemon без изменения `rsNomadNet`.

Внутренний `lxmf-tools` напрямую использует более широкую поверхность:
публичные поля `LxmRouter`, propagation store, peers, tickets, link delivery,
ratchets, transport messages и файловые persistence helpers. Его необходимо
переводить одновременно с внутренним backend, сохраняя CLI и control protocol.

## Политика совместимости

В ходе SQLite-миграции без отдельного breaking-change решения нельзя:

- менять LXMF wire encoding;
- менять значения protocol enums и constants;
- менять публичные поля/семантику `LxMessage`;
- менять `DeliveryIdentity` и message construction API;
- менять методы pack/unpack/encrypt/sign/verify;
- менять propagation protocol requests и responses;
- менять daemon control protocol;
- менять callback/event semantics;
- менять `PropagationClient` API, используемый `rsNomadNet`.

Допускается:

- заменить внутренние коллекции SQLite-backed storage;
- хранить metadata и payload одной записью SQLite;
- перестать создавать Python-compatible и `.lxm`-файлы;
- добавить owned, paginated и streaming API;
- сохранить старые ссылочные/full-list методы через compatibility facade;
- deprecated legacy persistence API перед удалением в следующем breaking
  release.

## Workspace crates

### `lxmf-core`

Публичные модули:

- `application`;
- `constants`;
- `delivery_ratchet`;
- `discovery_stamper`;
- `handlers`;
- `link_delivery`;
- `message`;
- `peer`;
- `persist`;
- `propagation`;
- `propagation_client`;
- `propagation_node`;
- `propagation_sync`;
- `router`;
- `stamper`;
- `sync`;
- `ticket`;
- `types`.

Это основной library API для embedded applications и `lxmf-tools`.

### `lxmf-tools`

Публичные модули:

- `daemon`;
- `lxmd_cli`;
- `lxmd_control`;
- `lxmd_runtime`.

Это API конфигурации, запуска daemon, CLI parsing, control encoding/decoding,
форматирования статуса и runtime paths. Файловая раскладка внутри
`LxmdPaths` является публичной и требует осторожной миграции.

## `application`

Публичные элементы:

- `DELIVERY_APP_NAME`;
- `DeliveryIdentity`;
- `ApplicationError`.

`DeliveryIdentity` предоставляет:

- `new`;
- `identity`;
- `destination`;
- `destination_hash`;
- `display_name`;
- `set_display_name`;
- `stamp_cost`;
- `announce_app_data`;
- `announce_packet`;
- `message`.

Эта поверхность напрямую используется `rsNomadNet` и должна сохраниться.
SQLite не должна становиться обязательным параметром `DeliveryIdentity`.

## `constants`

Модуль экспортирует:

- идентификаторы LXMF fields;
- audio codec и renderer IDs;
- nested field keys для reaction/comment/continuation;
- propagation metadata keys;
- compression signalling;
- `MessageState`;
- `DeliveryMethod`;
- `DeliveryRepresentation`;
- `UnverifiedReason`;
- размеры и protocol overhead;
- ticket, retry, expiry и storage timings;
- propagation/delivery/sync limits;
- stamp costs;
- peer/autopairing constants;
- control paths;
- sync/offer constants;
- `PeerState`, `PeerError`, `SyncStrategy`;
- job intervals;
- application/aspect names.

Все wire-visible числа, enum variants и paths являются стабильным protocol
contract. В частности, `DeliveryMethod` и `UnverifiedReason` используются
`rsNomadNet`.

## `types`

Публичные aliases:

- `DestinationHash`;
- `IdentityHash`;
- `MessageId`;
- `PropagationTransientId`;
- `PROPAGATION_TRANSIENT_ID_LENGTH`.

Формат hashes и длины менять нельзя.

## `message`

### Публичные типы

- `MessageCallback`;
- `MessageCallbacks`;
- `LxMessage`;
- `MessagePayload`;
- `MessageError`;
- serde helper module для fields.

### Основной API `LxMessage`

- `new`;
- compression capability selection;
- delivery/failed callbacks;
- `set_field`, `set_msgpack_field`, `get_field`;
- `pack_payload`, `pack`;
- opportunistic encrypted packing;
- propagated encrypted packing;
- propagated encrypted packing with stamp;
- propagation wrapper pack/unpack и bounded unpack;
- transient ID calculation;
- normal/propagated unpack;
- hash calculation;
- sign/sign-with callback/verify;
- stamp extraction и validation с tickets;
- state transitions: cancel, failed, rejected, delivered, sending, sent;
- paper URI encode/decode;
- generic URI encode/decode;
- container pack/unpack;
- legacy directory/file read/write.

### Совместимость

`LxMessage` содержит публичные поля, включая source/destination hashes,
timestamp, title/content, fields, method/state, attempts/progress, hashes,
stamp, wire payload и callbacks. Приложения могут конструировать и читать их
напрямую, поэтому удалять поля нельзя в non-breaking миграции.

Для outbound SQLite storage полное сообщение можно сериализовать внутренне, но
in-memory `LxMessage` остаётся прикладным DTO.

`get_field -> Option<&Vec<u8>>` не мешает SQLite: fields принадлежат уже
загруженному сообщению. Менять его на database reference не требуется.

`write_to_directory` и `read_from_file` являются legacy API. Они не
используются внешними загруженными приложениями, но должны быть deprecated до
удаления.

## `handlers`

Публичные типы:

- `HandlerType`;
- `AnnounceResult`;
- `CompressionSupport`;
- `PropagationNodeAnnounceData`;
- `ControlEndpoint`;
- `ControlResult`;
- `PropagationRequestHandler`.

Публичные announce helpers:

- propagation node app data encode/parse;
- delivery announce app data encode/parse;
- display name extraction;
- delivery/PN stamp cost extraction;
- compression support extraction;
- propagation node name extraction.

`display_name_from_app_data`, `pn_name_from_app_data` и
`parse_pn_announce_data` используются `rsNomadNet` и должны сохранять
сигнатуры и parsing semantics.

`PropagationRequestHandler` предоставляет stats/offer/get/sync/unpeer
handlers, peer throttling и control allow-list. Он работает через
`PropagationNode`; storage backend остаётся внутренним.

## `propagation_client`

Публичные типы:

- `PropagationClientState`;
- `PropagationClient`.

Основные методы:

- `new`;
- `set_runtime`;
- `set_propagation_node`;
- `set_delivery_limit`;
- добавление локальных transient IDs;
- чтение available messages;
- извлечение received messages;
- start download/all/specific;
- cancel;
- `tick`;
- state/progress/count/error accessors.

Эта поверхность напрямую используется `rsNomadNet`. В частности, enum states,
download lifecycle и формат `Vec<Vec<u8>>` received messages должны
сохраниться.

Внутренне received payload можно писать в прикладную БД или выдавать потоково
через новый API, но существующий `take_received_messages` остаётся.

## `propagation`

### `PropagationEntry`

Публичные metadata:

- transient ID;
- message hash;
- destination hash;
- stored timestamp;
- stamp value;
- size;
- collected/stamped flags.

Методы:

- `new`;
- `new_stamped`;
- `filename`;
- `parse_filename`.

`filename` и `parse_filename` относятся к старому файловому backend. Они не
используются внешними приложениями. При SQLite их следует deprecated, а затем
удалить вместе с file importer.

### `PropagationStore`

Публичные операции:

- `new`;
- `insert`;
- `get`;
- `contains`;
- `remove`;
- `transient_ids`;
- `entries`;
- `entries_for_destination`;
- expiry и weighted culling;
- weight calculation;
- stamp lookup;
- ignore/prioritise destination;
- locally delivered/processed mark/check;
- доступ/замена обеих transient maps;
- очистка transient caches;
- peer distribution enqueue/drain/check;
- `len`, `is_empty`, `total_size`, `iter`;
- storage limit.

Проблемные для SQLite методы:

- `get -> Option<&PropagationEntry>`;
- `entries -> Iterator<&PropagationEntry>`;
- `entries_for_destination -> Vec<&PropagationEntry>`;
- `locally_delivered_ids -> &HashMap`;
- `locally_processed_ids -> &HashMap`;
- `replace_*` с полным `HashMap`;
- `iter` со ссылками.

Загруженные внешние приложения их не используют. План совместимости:

1. внутренний код переводится на owned/page/query operations;
2. старый `PropagationStore` остаётся facade на ограниченный cache либо
   memory backend;
3. full-map методы помечаются memory-expensive/deprecated;
4. новый SQLite backend не загружает таблицы целиком при старте.

## `propagation_node`

Публичные типы:

- `PropagationNodeConfig`;
- `OfferRequestContext`;
- `GetRequestAction`;
- `GetServePlan`;
- `PlannedMessageRead`;
- `PropagationNode`.

Публичные операции:

- constructors/storage configuration;
- minimum stamp cost;
- accept normal/stamped propagation message;
- maintenance tick;
- offer creation/filtering;
- message count/size/contains;
- sync session get/start/remove;
- peer save/load;
- offer request and checked offer request;
- offer response encoding;
- offer/get request handling;
- planning and чтение message payload;
- peer sync prepare/process;
- handled/complete sync.

Текущий `with_storage` и planned reads принимают filesystem paths, а
`save_peer/load_peers` сохраняют отдельные файлы. Это внутренняя файловая
модель, не используемая внешними приложениями.

При SQLite:

- `PropagationNode::new` и high-level request API сохраняются;
- `with_storage` остаётся compatibility constructor либо начинает принимать
  directory, внутри которого создаётся database;
- `PlannedMessageRead` получает внутренний database key вместо обязательного
  file path;
- старый filesystem importer изолируется;
- payload читается из SQLite только при `serve/process_sync_get`.

Reference API `get_session/get_session_mut/start_session` относится к активным
короткоживущим sync sessions и остаётся в RAM.

## `router`

### Публичные типы

- `RouterConfig`;
- `RouterConfigExt`;
- `DeferredStampJob`;
- `SendError`;
- direct delivery planning types;
- `LxmRouter`;
- `DeliveryCallback`;
- `AutopeerCandidate`;
- `StampCostEntry`;
- outbound action/result/stats DTO.

### Основной high-level API

- `new`;
- transport attach/check;
- `send`, `try_send`;
- deferred stamp lifecycle;
- allow/block/control/ignore/prioritise policies;
- authentication and retain-node settings;
- message storage limit/size;
- ticket generation/lookup;
- outbound cancellation/result handling;
- peer management/sync;
- announce/propagation handlers;
- stats;
- throttling/peer rotation;
- outbound processing/action execution;
- job tick and storage cleanup.

### Публичные поля и риск совместимости

`LxmRouter` открывает внутреннее состояние напрямую:

- `pending_outbound: Vec<LxMessage>`;
- `pending_deferred_stamps: HashMap<..., LxMessage>`;
- `peers`;
- `propagation_store`;
- `outbound_stamp_costs`;
- `ticket_store`;
- policy lists/maps;
- transport sender;
- runtime/deferred job state;
- counters и propagation status.

Это главный representation leak. `lxmf-tools` использует часть полей напрямую.
Внешний `rsNomadNet` `LxmRouter` не использует.

Для SQLite необходимо:

- сначала перевести `lxmf-tools` на методы;
- добавить query/update methods вместо прямого доступа;
- временно сохранить поля для memory compatibility mode;
- не поддерживать две неограниченные копии данных;
- удалять/закрывать поля только в отдельном major release.

Сохранить абсолютно те же публичные `Vec`/`HashMap` как источник истины и
одновременно перестать держать данные в RAM невозможно.

## `ticket`

Публичные типы:

- `Ticket`;
- `TicketStore`.

Методы:

- ticket construction/validity/renewal;
- store new/add/find/cull/count;
- `all`;
- `replace_all`.

`find -> Option<&Ticket>` и `all -> &[Ticket]` предполагают resident `Vec`.
Внешние приложения их не используют. Для SQLite добавляются owned lookup и
page iteration; старый store может остаться небольшим cache/facade.

## `peer`

`LxmPeer` содержит propagation peer identity, costs, sync timestamps,
statistics, handled message set и active transfer state.

Публичны:

- constructors/from announce;
- stamp cost helpers;
- unhandled/handled message operations;
- serialization with handled IDs;
- liveness/backoff/staleness;
- peering key generation;
- acceptance rate;
- sync/link lifecycle;
- `select_sync_peer`.

Активные peer/link поля остаются в RAM. Долгосрочный handled-message set и
serialized peer record могут быть перенесены в SQLite после propagation store.

## `sync` и `propagation_sync`

`sync` экспортирует protocol constants, `SyncOffer`, `SyncGet`,
`OfferResponse`, `SyncSession`, `SyncState` и offer/get/session operations.

`propagation_sync` экспортирует:

- `SyncTaskState`;
- `PropagationSyncTask`;
- construction/storage/shared-node configuration;
- runtime/identity/node selection;
- request/tick/event processing;
- accepted message count и peer access.

Wire representation и state machine сохраняются. Payload source меняется с
файла на SQLite behind the same high-level behavior.

## `link_delivery`

Публичны:

- delivery state/result/error/report DTO;
- backchannel commands/errors/reports;
- delivery events и snapshots;
- `LinkDeliveryStats`;
- `LinkDeliveryManager`;
- re-exported runtime link-manager receipt/proof types.

Manager API включает:

- construction/runtime/channel setup;
- backchannel registration;
- direct/packed/backchannel delivery;
- drain/tick;
- packet/resource proof handling;
- pending/cancel/fail;
- event/snapshot/stats access;
- session count.

Это активное runtime state и не переносится в SQLite. Outbound message record
хранится в БД, но активная link/resource transfer остаётся в памяти.

## `delivery_ratchet`

Публичны:

- `DELIVERY_APP_NAME`;
- `DeliveryAnnounceKind`;
- `DeliveryRatchetError`;
- `DeliveryRatchetState`.

API включает load/initialize, ring/control access, announce creation, save и
path accessors. Текущие отдельные файлы являются legacy storage. Внешний
`rsNomadNet` использует delivery identity, но не этот state object.

После переноса ratchets в SQLite high-level announce behavior сохраняется;
path getters deprecated либо возвращают compatibility/export paths.

## `stamper` и `discovery_stamper`

`stamper` экспортирует:

- workblock generation;
- stamp value/validation;
- iteration cap;
- normal/limited generation;
- peering и propagation stamp validation;
- deferred stamp handle/result/spawn.

`discovery_stamper` экспортирует:

- iteration/expand constants;
- `LxmfDiscoveryStamper`;
- discovery stamp generation.

Эти CPU-bound API не относятся к storage migration и сохраняются.

## `persist`

Публичны имена файлов и functions:

- atomic write;
- save/load stamp costs;
- save/load tickets;
- save/load local deliveries;
- save/load locally processed IDs.

После SQLite это legacy API. Оно не используется внешними приложениями.
Порядок удаления:

1. прекратить внутреннее использование;
2. при необходимости импортировать старые данные один раз;
3. пометить функции deprecated;
4. удалить в следующем breaking release.

Новые изменения не должны одновременно писать SQLite и старые snapshot files.

## `lxmf-tools`

### `daemon`

Публичны:

- Python/daemon config structures;
- conversion из `rns_runtime::Config`;
- `create_router`;
- `create_router_with_transport`;
- inbound command execution.

CLI/config behavior сохраняется. SQLite path/cache/maintenance параметры
добавляются с defaults.

### `lxmd_cli`

Публичны:

- `SendMethod`;
- CLI `Args`;
- JSON fields parsing;
- hash normalization/parsing;
- example config;
- hash-list loading.

### `lxmd_control`

Публичный control codec и request handling должен сохранять endpoint payloads,
errors, status/peer representations и encrypted control behavior.
Внутреннее получение stats/messages меняется на storage queries.

### `lxmd_runtime`

Публичны:

- `LxmdPaths`;
- local status/peer views и formatters;
- delivery/propagation announce helpers;
- config directory resolution;
- control preflight types/functions.

`LxmdPaths` раскрывает активные пути конфигурации, identity и
`storage/lxmf/lxmf.sqlite`, а также отдельный legacy fallback для identity.
Поля путей message/ratchet/state files удалены: файловая совместимость storage
не поддерживается, база всегда создаётся с чистого листа.

## Фактические потребители

### `rsNomadNet`

Прямой dependency: `lxmf-core`.

Использует:

- построение local delivery identity;
- delivery announce destination;
- создание opportunistic/direct/propagated messages;
- pack и encrypted pack;
- stamp generation для propagated message;
- unpack/decrypt входящих сообщений;
- validation state/reason;
- propagation client download;
- delivery и propagation announce parsing.

Не использует router/daemon/storage API.

### `rsReticulum`

Не зависит от `lxmf-core`; только документирует extension seam
`DiscoveryStamper`, реализуемый `LxmfDiscoveryStamper`.

### `rsNodePage`, `rsRRC`, `rsRRC-client`, `rsRRCD`

Прямой зависимости от `rsLXMF` нет.

### `lxmf-tools` и examples

Это внутренние consumers:

- daemon непосредственно владеет `LxmRouter`;
- обращается к router fields и propagation node;
- управляет link delivery, ratchets, peers и persistence;
- examples используют identity, messages, runtime link delivery и transport.

Их компиляция и функциональные тесты обязательны после каждого storage этапа.

## Compatibility matrix

| API | Внешнее использование | Решение |
|---|---:|---|
| `DeliveryIdentity` | Да | Сохранить полностью |
| `LxMessage`/wire methods | Да | Сохранить полностью |
| Constants/enums | Да | Не менять |
| Announce parsers | Да | Сохранить полностью |
| `PropagationClient` | Да | Сохранить полностью |
| `LxmRouter` methods | Внутреннее tools | Сохранить high-level |
| Публичные поля `LxmRouter` | Внутреннее tools | Перевести tools, затем deprecated |
| `PropagationStore` reference API | Нет внешнего | Facade/owned replacement |
| `TicketStore` slice/reference API | Нет внешнего | Facade/owned replacement |
| `PropagationNode` filesystem API | Нет внешнего | Compatibility constructor/importer |
| `persist` snapshots | Нет внешнего | Deprecated, затем удалить |
| `LxMessage` file helpers | Нет внешнего | Deprecated, затем удалить |
| Link delivery active state | Внутреннее | Оставить в RAM |
| Daemon control/CLI | Пользовательский | Сохранить поведение |

## Рекомендуемые новые API

Перед непосредственной миграцией полезно добавить:

- `LxmfStorage` abstraction;
- owned propagation entry lookup;
- paginated destination/message queries;
- metadata lookup без payload;
- отдельную загрузку payload по transient ID;
- SQL-backed count/total size;
- transient ID mark/check/delete-before;
- outbound ready-page query;
- explicit router policy/query methods вместо public field access;
- storage health/statistics;
- `LxmdPaths::database_path`;
- compatibility compile tests для `rsNomadNet`.

Существующие методы остаются, пока внутренние consumers не переведены на новый
API.

## Вывод

SQLite migration можно выполнить без изменений `rsNomadNet`, если сохранить
`DeliveryIdentity`, `LxMessage`, announce parsers и `PropagationClient`.

Главное препятствие находится не во внешнем прикладном API, а внутри
`lxmf-tools`: `LxmRouter` раскрывает `Vec`/`HashMap` и store objects публичными
полями. Первый рефакторинг должен перевести daemon на high-level methods и
storage abstraction. После этого metadata, payload, transient IDs и outbound
queue можно хранить по требованию в SQLite, оставляя активные link/sync jobs в
RAM.
