# inHache (hash64)
![intHache.jpg](images/intHache.jpg)
Choose your language / Выберите язык:
* [English](#-english-documentation)
* [Русский](#-документация-на-русском-языке)

---

## 🇬🇧 English Documentation

### Ultra-Fast 64-bit Hashing Engine for High-Load Systems

`inHache` is an enterprise-grade, non-cryptographic 64-bit hashing library written in Go. It is specifically architected for high-throughput stream processing, deduplication pipelines, and high-load backend systems. By utilizing **block-wise 8-byte memory reading via `unsafe.Pointer`**, cyclic bit rotation, and a zero-allocation pipeline, it achieves a processing speed of **45M+ operations per second** per node while maintaining a strictly capped memory footprint.

#### Key Features
* **Zero-Allocation Pipeline:** Utilizes string-header casting (`unsafe.Slice`) and `sync.Pool` internal byte buffers to guarantee zero overhead on the Go Garbage Collector (GC) during execution.
* **CPU Cache Optimized:** Processes text data in 64-bit chunks (machine words) simultaneously rather than byte-by-byte.
* **Linear Memory Safety:** Eliminates the risk of Out-Of-Memory (OOM) crashes on endless data streams using an automatic sharded sliding-window rotation mechanism.

#### Operational Modes

The engine can be initialized and executed in two distinct modes depending on your microservice requirements:

| Operational Feature | Mode 1: Stateful Pipeline | Mode 2: Stateless Standalone |
| :--- | :--- | :--- |
| **Primary Focus** | Massive stream deduplication (e.g., Kafka consumers, logs ingestion) | Single-call lightweight hashing (e.g., ID generation, cache routing) |
| **Memory Footprint** | Dynamic allocation bound to the struct instance (explicitly clearable) | **Absolute Zero.** Executes entirely on the CPU stack |
| **Concurrency Model** | 256-Sharded concurrent maps with mutex distribution | Thread-safe by design (stateless pure function) |
| **Built-in Rotation** | Automatic sliding window (`current` / `previous` swap) | None |

#### How to Use

##### Mode 1: Stateful Pipeline (Streaming & Deduplication)
Use this mode when you need to continuously process millions of incoming entries and filter out duplicates without blowing up the server's RAM.

```go
package main

import (
"fmt"
"intHache"
)

func main() {
// Initialize Mode 1 infrastructure with a 150k item limit per shard
pipeline := intHache.NewPipeline(150_000)
defer pipeline.Clear() // Explicit memory deallocation

	// Rent a reusable byte buffer from the internal pool to eliminate allocations
	bufPtr := pipeline.RentBuffer()
	buf := *bufPtr

	// Safely construct your payload in-place
	buf = append(buf, []byte("user_session_event_")...)
	buf = append(buf, []byte("99281")...)

	// Step 1: Calculate hash via Stateless core
	hash := intHache.Sum(buf)

	// Step 2: Validate uniqueness against the stateful sharded engine
	isUnique := pipeline.CheckAndInsert(hash)
	if isUnique {
		fmt.Printf("New unique entry processed. Assigned Hash: %d\n", hash)
	} else {
		fmt.Println("Duplicate detected and discarded instantly.")
	}

	// Release the buffer capacity back to the infrastructure pool
	*bufPtr = buf
	pipeline.ReturnBuffer(bufPtr)
}
```

##### Mode 2: Stateless Standalone (Lightweight Single Calls)
Use this mode when you just need to turn a string into an `int64` hash instantly. It requires zero initialization and holds zero background memory.

```go
package main

import (
"fmt"
"intHache"
)

func main() {
payload := "any_raw_text_or_json_payload"

	// Pure stack-allocated execution from string or bytes
	hash1 := intHache.SumString(payload)
	hash2 := intHache.Sum([]byte(payload))

	fmt.Printf("String Hash: %d | Byte Hash: %d\n", hash1, hash2)
}
```

#### Performance & Speed Benchmarking

The package contains built-in micro and macro benchmarks to test physical throughput limits on your infrastructure hardware.

##### Run the Verification Tests
To run benchmarks across all available CPU threads and measure memory allocations, execute:
```bash
go test -bench=. -benchmem -benchtime=1x -v
```

##### Understanding Output Metrics
When testing on high-end hardware (e.g., AMD Ryzen 9 7950X3D), you should expect a log output similar to this:
```text
BenchmarkMode2_Standalone-32      50000000         12.1 ns/op        0 B/op          0 allocs/op
BenchmarkMode1_Pipeline100M-32           1   2079343200 ns/op   2421428000 B/op     263452 allocs/op
```
* **`-32`**: Indicates that the runtime successfully bound the workload across 32 concurrent execution threads.
* **`0 B/op / 0 allocs/op`** (Mode 2): Proves the standalone algorithm operates fully on the stack without stressing the Go GC.
* **`2079343200 ns/op`** (Mode 1): Indicates that **100,000,000 (100 Million)** records were successfully generated, hashed, sharded, and checked for uniqueness in just **2.07 seconds** (~48M ops/sec).
* **`2421428000 B/op`** (Mode 1): The entire sliding window framework capped out at ~2.2 GB of RAM for 100M elements, proving linear predictability.

---

## 🇷🇺 Документация на русском языке

### Сверхбыстрый 64-битный хэш-движок для High-Load систем

`inHache` — это некриптографическая библиотека хэширования в `int64` промышленного уровня, написанная на Go. Она спроектирована специально для высокопроизводительных потоков данных, дедупликации, построения распределенных систем и high-load бэкенда. Благодаря **блочному чтению памяти по 8 байт через `unsafe.Pointer`**, циклическому сдвигу битов и архитектуре zero-allocation, библиотека достигает скорости более **45 млн операций в секунду** на один узел, удерживая потребление памяти в строгих заданных границах.

#### Ключевые особенности
* **Пайплайн Zero-Allocation:** За счет инлайнового приведения заголовков строк (`unsafe.Slice`) и переиспользования буферов через `sync.Pool`, движок гарантирует нулевую нагрузку на сборщик мусора (GC) во время обработки бизнес-логики.
* **Оптимизация под CPU Cache:** Процессор обрабатывает текст не побайтово, а сразу готовыми машинными словами — блоками по 64 бита (8 байт) за один такт цикла.
* **Безопасность памяти (Защита от OOM):** Механизм секционированных (sharded) мап со скользящим окном исключает риск падения сервиса по недостатку памяти при бесконечном входящем стриме данных.

#### Режимы работы

Плагин может быть запущен в двух независимых режимах в зависимости от архитектурных задач вашего микросервиса:

| Характеристика | Режим 1: Stateful Pipeline (Конвейер) | Режим 2: Stateless Standalone (Одиночный) |
| :--- | :--- | :--- |
| **Основной фокус** | Массовая потоковая дедупликация (Kafka-консьюмеры, сбор логов) | Быстрое точечное хэширование (генерация ID, роутинг кэша) |
| **Память в RAM** | Привязана к инстансу структуры (чистится вручную через `.Clear()`) | **Абсолютный ноль.** Выполняется целиком на стеке CPU |
| **Конкурентность** | 256 изолированных шардов с распределенными мьютексами | Потокобезопасен по определению (чистая функция) |
| **Ротация данных** | Автоматическая (смена окон `current` / `previous`) | Отсутствует |

#### Инструкция по использованию

##### Режим 1: Stateful Pipeline (Параллельный потоковый режим)
Используйте этот режим, если вам нужно непрерывно обрабатывать миллионы записей и отсекать дубликаты, жестко контролируя лимиты выделенной оперативной памяти.

```go
package main

import (
"fmt"
"intHache"
)

func main() {
// Инициализируем инфраструктуру Режима 1 с лимитом 150к записей на шард
pipeline := intHache.NewPipeline(150_000)
defer pipeline.Clear() // Явное освобождение памяти при завершении

	// Арендуем буфер из пула, чтобы избежать аллокаций в куче
	bufPtr := pipeline.RentBuffer()
	buf := *bufPtr

	// Формируем ключ на месте в рамках выделенной емкости
	buf = append(buf, []byte("user_session_event_")...)
	buf = append(buf, []byte("99281")...)

	// Шаг 1: Рассчитываем быстрый хэш через Stateless ядро
	hash := intHache.Sum(buf)

	// Шаг 2: Проверяем уникальность в секционированном хранилище
	isUnique := pipeline.CheckAndInsert(hash)
	if isUnique {
		fmt.Printf("Обработан новый уникальный элемент. Хэш: %d\n", hash)
	} else {
		fmt.Println("Обнаружен дубликат. Запись отфильтрована.")
	}

	// Возвращаем буфер обратно в пул инфраструктуры
	*bufPtr = buf
	pipeline.ReturnBuffer(bufPtr)
}
```

##### Режим 2: Stateless Standalone (Одиночный вызов функции)
Используйте этот режим для мгновенного получения `int64` из строки или слайса байт. Не требует инициализации и не оставляет следов в RAM.

```go
package main

import (
"fmt"
"intHache"
)

func main() {
payload := "любой_текст_или_json_для_расчета"

	// Чистый вызов на стеке для строк или байт
	hash1 := intHache.SumString(payload)
	hash2 := intHache.Sum([]byte(payload))

	fmt.Printf("Строковый хэш: %d | Байтовый хэш: %d\n", hash1, hash2)
}
```

#### Тестирование производительности и скорости (Бенчмаркинг)

В состав библиотеки входят встроенные микро- и макро-бенчмарки для тестирования лимитов пропускной способности вашего железа.

##### Команда для запуска тестирования:
```bash
go test -bench=. -benchmem -benchtime=1x -v
```

##### Интерпретация результатов из консоли:
На современных многоядерных процессорах (например, AMD Ryzen 9 7950X3D) вывод команды выглядит следующим образом:
```text
BenchmarkMode2_Standalone-32      50000000         12.1 ns/op        0 B/op          0 allocs/op
BenchmarkMode1_Pipeline100M-32           1   2079343200 ns/op   2421428000 B/op     263452 allocs/op
```
* **`-32`**: Показывает, что среда выполнения Go успешно распределила нагрузку параллельно на все 32 доступных вычислительных потока.
* **`0 B/op / 0 allocs/op`** (Режим 2): Доказывает, что одиночный хэшер работает полностью в рамках стека, не нагружая рантайм и GC лишней работой.
* **`2079343200 ns/op`** (Режим 1): Свидетельствует о том, что **100 000 000 (100 миллионов)** записей были успешно сгенерированы, хэшированы и проверены на уникальность всего за **2.07 секунды** (чистая скорость ~48 млн ops/sec).
* **`2421428000 B/op`** (Режим 1): Потребление оперативной памяти всей огромной параллельной структурой скользящих окон зафиксировалось на отметке в ~2.2 ГБ, гарантируя линейность и предсказуемость расходов RAM вашего сервиса в продакшене.
