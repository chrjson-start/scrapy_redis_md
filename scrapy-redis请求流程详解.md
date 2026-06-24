# scrapy-redis 请求流程详解
在scrapy-redis重写的start_requests()方法中，会调用next_requests()方法，初始化设置后从Redis队列中取出数据，
并调用make_request_from_data()方法，redis中保存的是字节数据，需要转为json字符串。
，将数据转换成Scrapy Request对象，
    return FormRequest(
                url, dont_filter=True, method=method, formdata=parameter, meta=metadata
            )#post请求是表单数据，不重写是不过滤的，重写make_request_from_data()方法要使用return返回Request对象
最后用 yield 返回。

## 整体流程图

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         scrapy-redis 请求流程                            │
└─────────────────────────────────────────────────────────────────────────┘

    ┌──────────────┐
    │ start_requests() │  ① 入口方法，由 Scrapy 引擎自动调用
    └──────┬───────┘
           │
           │ 直接调用
           ↓
    ┌──────────────┐
    │ next_requests() │  ② 从 Redis 队列批量取出数据
    └──────┬───────┘
           │
           │ for 循环遍历数据
           ↓
    ┌────────────────────────┐
    │ make_request_from_data() │  ③ 将 Redis 数据转换成 Request 对象
    └────────────────────────┘
           │
           │ yield 返回
           ↓
    ┌──────────────┐
    │   Request    │  ④ 生成 Scrapy Request 对象，加入调度器
    └──────────────┘
```

---

## 详细代码分析

### 1. RedisMixin 类（核心混入类）

```python
class RedisMixin:
    """Mixin class to implement reading urls from a redis queue."""
    # 类的属性定义
    redis_key = None              # Redis 队列的键名（必须定义）
    redis_batch_size = None       # 每次从 Redis 批量获取的数量
    redis_encoding = None         # Redis 数据的编码格式（默认 utf-8）
    
    server = None                 # Redis 客户端连接对象
    spider_idle_start_time = int(time.time())  # 爬虫空闲开始时间
    max_idle_time = None          # 最大空闲时间，超过则关闭爬虫
```

---

### 2. start_requests() - 入口方法

```python
def start_requests(self):
    """Returns a batch of start requests from redis."""
    # 简单直接：调用 next_requests() 获取请求
    return self.next_requests()
```

**作用**：
- 这是 Scrapy 引擎自动调用的第一个方法
- 在 scrapy-redis 中，直接委托给 `next_requests()`
- 普通 Scrapy 的 `start_requests()` 返回 `start_urls` 列表
- scrapy-redis 的 `start_requests()` 返回从 Redis 队列获取的 Request 生成器

---

### 3. setup_redis() - Redis 连接初始化

```python
def setup_redis(self, crawler=None):
    """Setup redis connection and idle signal."""
    
    # 如果已经设置过 Redis 连接，直接返回（避免重复初始化）
    if self.server is not None:
        return
    
    # 获取爬虫的 crawler 对象（用于访问 settings）
    if crawler is None:
        crawler = getattr(self, "crawler", None)
    
    if crawler is None:
        raise ValueError("crawler is required")
    
    settings = crawler.settings
    
    # 如果没有定义 redis_key，从 settings 获取默认值
    # 默认值: "<spider.name>:start_urls"
    if self.redis_key is None:
        self.redis_key = settings.get(
            "REDIS_START_URLS_KEY",
            defaults.START_URLS_KEY,
        )
    
    # 支持 redis_key 中使用占位符 {name}，替换为爬虫名称
    self.redis_key = self.redis_key % {"name": self.name}
    
    # 验证 redis_key 不能为空
    if not self.redis_key.strip():
        raise ValueError("redis_key must not be empty")
    
    # 从 settings 获取批量大小，默认使用 CONCURRENT_REQUESTS
    if self.redis_batch_size is None:
        self.redis_batch_size = settings.getint(
            "CONCURRENT_REQUESTS", defaults.REDIS_CONCURRENT_REQUESTS
        )
    
    # 确保 redis_batch_size 是整数类型
    try:
        self.redis_batch_size = int(self.redis_batch_size)
    except (TypeError, ValueError):
        raise ValueError("redis_batch_size must be an integer")
    
    # 获取 Redis 编码格式，默认 utf-8
    if self.redis_encoding is None:
        self.redis_encoding = settings.get(
            "REDIS_ENCODING", defaults.REDIS_ENCODING
        )
    
    # 日志输出 Redis 配置信息
    self.logger.info(
        "Reading start URLs from redis key '%(redis_key)s' "
        "(batch size: %(redis_batch_size)s, encoding: %(redis_encoding)s)",
        self.__dict__,
    )
    
    # 建立 Redis 连接
    self.server = connection.from_settings(crawler.settings)
    
    # 根据 settings 配置，选择不同的 Redis 数据结构
    # 1. 使用 SET (SPOP) - 无序集合
    if settings.getbool("REDIS_START_URLS_AS_SET", defaults.START_URLS_AS_SET):
        self.fetch_data = self.server.spop    # 随机弹出一个元素
        self.count_size = self.server.scard   # 获取集合大小
    
    # 2. 使用 ZSET (Sorted Set) - 有序集合，支持优先级
    elif settings.getbool("REDIS_START_URLS_AS_ZSET", defaults.START_URLS_AS_ZSET):
        self.fetch_data = self.pop_priority_queue  # 自定义优先级队列
        self.count_size = self.server.zcard        # 获取有序集合大小
    
    # 3. 使用 LIST (LPOP) - 列表（默认）
    else:
        self.fetch_data = self.pop_list_queue   # LPOP 弹出
        self.count_size = self.server.llen      # 获取列表长度
    
    # 获取最大空闲时间
    if self.max_idle_time is None:
        self.max_idle_time = settings.get(
            "MAX_IDLE_TIME_BEFORE_CLOSE", defaults.MAX_IDLE_TIME
        )
    
    try:
        self.max_idle_time = int(self.max_idle_time)
    except (TypeError, ValueError):
        raise ValueError("max_idle_time must be an integer")
    
    # 注册空闲信号处理器
    # 当爬虫空闲（没有更多请求）时，自动从 Redis 取新请求
    crawler.signals.connect(self.spider_idle, signal=signals.spider_idle)
```

---

### 4. next_requests() - 核心：从 Redis 批量获取请求

```python
def next_requests(self):
    """Returns a request to be scheduled or none."""
    found = 0  # 计数器：成功获取的请求数量
    
    # 从 Redis 批量获取数据
    # fetch_data 的具体实现取决于 setup_redis 中的配置：
    # - pop_list_queue: LPOP + LTRIM（列表）
    # - pop_priority_queue: ZREVRANGE + ZREMRANGEBYRANK（有序集合）
    # - server.spop（集合）
    # self.fetch_data 是根据配置选择的函数，用于从 Redis 获取数据
    # 从 Redis 获取数据，返回一个列表（或可迭代对象）
    datas = self.fetch_data(self.redis_key, self.redis_batch_size)
    
    # 遍历获取到的每一批数据
    for data in datas:
        # 调用 make_request_from_data 将数据转换成 Request
        reqs = self.make_request_from_data(data)
        
        # 如果返回的是一个可迭代对象（多个请求）
        if isinstance(reqs, Iterable):
            for req in reqs:
                yield req  # 逐个 yield 请求
                found += 1
                self.logger.info(f"start req url:{req.url}")
        
        # 如果只返回一个请求
        elif reqs:
            yield reqs
            found += 1
        
        # 如果返回空（无效数据）
        else:
            self.logger.debug(f"Request not made from data: {data}")
    
    # 如果成功获取了请求，记录日志
    if found:
        self.logger.debug(f"Read {found} requests from '{self.redis_key}'")
```

---

### 5. make_request_from_data() - 将 Redis 数据转换成 Request

```python
def make_request_from_data(self, data):
    """Returns a `Request` instance for data coming from Redis."""
    
    # data 是从 Redis 获取的原始数据（bytes 类型）
    # bytes_to_str: 将字节串转换成字符串
    formatted_data = bytes_to_str(data, self.redis_encoding)
    
    # 判断数据格式：字典（JSON）还是普通字符串
    if is_dict(formatted_data):
        # JSON 格式：支持更丰富的配置
        parameter = json.loads(formatted_data)  # 解析 JSON
        
        # 检查是否有 url 字段
        if parameter.get("url", None) is None:
            self.logger.warning(
                f"{TextColor.WARNING}The data from Redis has no url key{TextColor.ENDC}"
            )
            return []  # 返回空列表
        
        # 提取必要参数
        url = parameter.pop("url")              # URL（必填）
        method = parameter.pop("method").upper() if "method" in parameter else "GET"  # 请求方法
        metadata = parameter.pop("meta") if "meta" in parameter else {}               # 元数据
        
        # 创建 FormRequest
        # parameter 剩余字段作为 formdata（POST 请求体）
        return FormRequest(
            url, 
            dont_filter=True,     # 不过滤（因为是多节点分布式）
            method=method,         # GET 或 POST
            formdata=parameter,    # 表单数据
            meta=metadata          # 传递给后续请求的元数据
        )
    
    else:
        # 字符串格式（已废弃，不推荐）
        self.logger.warning(
            f"{TextColor.WARNING}String request is deprecated, "
            f"please use JSON data format.{TextColor.ENDC}"
        )
        return FormRequest(formatted_data, dont_filter=True)
```

---

### 6. schedule_next_requests() - 调度下一个批次的请求

```python
def schedule_next_requests(self):
    """Schedules a request if available"""
    # 遍历 next_requests() 返回的所有请求
    for req in self.next_requests():
        # 根据 Scrapy 版本选择不同的调度方式
        if scrapy_version >= (2, 6):
            # Scrapy 2.6+ 的新 API
            self.crawler.engine.crawl(req)
        else:
            # 旧版本的 API
            self.crawler.engine.crawl(req, spider=self)
```

---

### 7. spider_idle() - 空闲时自动调度

```python
def spider_idle(self):
    """
    Schedules a request if available, otherwise waits.
    or close spider when waiting seconds > MAX_IDLE_TIME_BEFORE_CLOSE.
    """
    
    # 如果 Redis 队列中还有数据，更新空闲开始时间
    if self.server is not None and self.count_size(self.redis_key) > 0:
        self.spider_idle_start_time = int(time.time())
    
    # 自动调度下一批请求
    self.schedule_next_requests()
    
    # 计算空闲时长
    idle_time = int(time.time()) - self.spider_idle_start_time
    
    # 如果超过最大空闲时间，允许关闭爬虫
    if self.max_idle_time != 0 and idle_time >= self.max_idle_time:
        return
    
    # 抛出异常阻止爬虫关闭（继续保持监听）
    raise DontCloseSpider
```

---

## 数据存储方式对比

### 1. LIST（列表）- 默认方式

```python
# 使用 LPOP + LTRIM 原子操作
# LPOP: 从列表左边弹出元素
# LTRIM: 裁剪列表，保留剩余部分
def pop_list_queue(self, redis_key, batch_size):
    with self.server.pipeline() as pipe:
        pipe.lrange(redis_key, 0, batch_size - 1)  # 获取前 batch_size 个元素
        pipe.ltrim(redis_key, batch_size, -1)      # 删除这些元素
        datas, _ = pipe.execute()
    return datas
```

```
Redis LIST:
before: [url1, url2, url3, url4, url5]
                ↑
              LPOP

after:  [url4, url5]
```

### 2. ZSET（有序集合）- 支持优先级

```python
# 按分数（优先级）从高到低获取
def pop_priority_queue(self, redis_key, batch_size):
    with self.server.pipeline() as pipe:
        pipe.zrevrange(redis_key, 0, batch_size - 1)  # 获取分数最高的元素
        pipe.zremrangebyrank(redis_key, -batch_size, -1)  # 删除这些元素
        datas, _ = pipe.execute()
    return datas
```

### 3. SET（集合）- 无序

```python
# 随机弹出一个元素
self.fetch_data = self.server.spop
```

---

## 完整请求生命周期

```
┌─────────────────────────────────────────────────────────────────────┐
│                          请求生命周期                                  │
└─────────────────────────────────────────────────────────────────────┘

    ① 启动爬虫
       │
       ↓
    ② RedisSpider.from_crawler() 被调用
       │
       ├─→ setup_redis() 初始化 Redis 连接
       │
       ↓
    ③ 爬虫启动完毕，Scrapy 引擎调用 start_requests()
       │
       ├─→ start_requests() 
       │      │
       │      ↓
       │   next_requests()
       │      │
       │      ├─→ fetch_data() 从 Redis 获取数据
       │      │
       │      ↓
       │   for data in datas:
       │      │
       │      ↓
       │   make_request_from_data(data)
       │      │
       │      ├─→ bytes_to_str() 字节转字符串
       │      ├─→ json.loads() 或直接返回字符串
       │      └─→ return FormRequest / Request
       │
       ↓
    ④ Request 对象加入调度器 (Scrapy Scheduler)
       │
       ↓
    ⑤ 引擎启动，Request 被下载器执行
       │
       ↓
    ⑥ 下载完成，回调 parse() 方法
       │
       ├─→ yield item → Item Pipeline
       │
       └─→ yield request → 回到调度器（可在此处也从 Redis 加载）
       
    ⑦ 当所有请求处理完毕，spider_idle() 信号触发
       │
       ├─→ schedule_next_requests()
       │
       └─→ 从 Redis 获取下一批 URL，继续执行
```

---

## RedisSpider 完整类定义

```python
class RedisSpider(RedisMixin, Spider):
    """Spider that reads urls from redis queue when idle.
    
    Attributes
    ----------
    redis_key : str
        Redis key where to fetch start URLs from
    redis_batch_size : int
        Number of messages to fetch from redis on each attempt
    redis_encoding : str
        Encoding to use when decoding messages from redis queue
    
    Settings
    --------
    REDIS_START_URLS_KEY : str
        Default key: "<spider.name>:start_urls"
    REDIS_START_URLS_AS_SET : bool
        Use SET operations (default: False)
    REDIS_ENCODING : str
        Default: "utf-8"
    """
    
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """类方法：Scrapy 自动调用来创建爬虫实例"""
        obj = super().from_crawler(crawler, *args, **kwargs)
        obj.setup_redis(crawler)  # 初始化 Redis 连接
        return obj
```

---

## 自定义示例

### 重写 make_request_from_data() 处理简单字符串 URL

```python
from scrapy_redis.spiders import RedisSpider
import scrapy

class MySpider(RedisSpider):
    name = 'my_spider'
    redis_key = 'my_spider:start_urls'
    
    def make_request_from_data(self, data):
        """重写：从 Redis 获取字符串 URL"""
        url = data.decode('utf-8')  # bytes 转字符串
        return scrapy.Request(url, callback=self.parse)  # 返回 Request 对象
```

### 重写 make_request_from_data() 处理 JSON 格式

```python
from scrapy_redis.spiders import RedisSpider
import scrapy

class MySpider(RedisSpider):
    name = 'my_spider'
    redis_key = 'my_spider:start_urls'
    
    def make_request_from_data(self, data):
        """重写：处理 JSON 格式数据"""
        import json
        
        formatted_data = bytes_to_str(data, self.redis_encoding)
        parameter = json.loads(formatted_data)
        
        url = parameter.get('url')
        meta = parameter.get('meta', {})
        
        if url:
            return scrapy.Request(url, meta=meta, callback=self.parse)
        return None
```

---

## 关键点总结

| 方法 | 作用 | 是否需要重写 |
|------|------|-------------|
| `start_requests()` | 入口方法，调用 `next_requests()` | ❌ 通常不需要 |
| `next_requests()` | 从 Redis 批量获取数据 | ❌ 通常不需要 |
| `make_request_from_data()` | 将 Redis 数据转换成 Request | ✅ **通常需要重写** |
| `setup_redis()` | 初始化 Redis 连接 | ❌ 不需要 |
| `spider_idle()` | 空闲时自动调度 | ❌ 不需要 |

---

## 为什么要重写 make_request_from_data()？

因为 scrapy-redis 默认的 `make_request_from_data()` 支持两种格式：

1. **JSON 格式**（推荐）：
   ```json
   {"url": "https://example.com", "meta": {"key": "value"}}
   ```

2. **字符串格式**（已废弃）：
   ```
   https://example.com
   ```

如果你需要：
- 使用 `scrapy.Request` 而不是 `FormRequest`
- 自定义请求参数
- 处理复杂的数据结构

就需要重写这个方法。
