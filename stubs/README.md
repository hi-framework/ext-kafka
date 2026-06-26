# hi-kafka IDE / 静态分析 stubs

`hi-kafka` PHP 扩展的类型声明文件，专供 IDE 自动补全与静态分析使用。

**不要在运行时 `require`** —— 真正的实现由 .so 提供，require 会撞 fatal "Cannot redeclare"。

## 集成

### PHPStorm

1. **菜单 → File → Settings → PHP → Include Path**，加上 `stubs/` 绝对路径
2. 右键 `stubs/hi_kafka.php` → **Mark as → PHP Reference File**

`Hi\Kafka\Client`、`hi_kafka_*` 全部能补全 + 类型推断 + 文档悬浮。

### VSCode（Intelephense）

`.vscode/settings.json`：

```json
{
    "intelephense.environment.includePaths": ["stubs"],
    "intelephense.stubs": ["Core", "standard", "swoole", "swow"]
}
```

或单独：

```json
{
    "intelephense.environment.includePaths": ["vendor/hi/kafka-stubs"]
}
```

### PHPStan

`phpstan.neon`：

```yaml
parameters:
    scanFiles:
        - stubs/hi_kafka.php

    # 或者用 composer 包后：
    # phpstan 会自动从 hi/kafka-stubs 的 composer.json extra.phpstan.stubFiles 读到
```

### Psalm

`psalm.xml`：

```xml
<psalm>
    <stubs>
        <file name="stubs/hi_kafka.php"/>
    </stubs>
</psalm>
```

## 类型注解约定

- 关联数组 / array shape 用 PHPStorm 的 [array shape](https://www.jetbrains.com/help/phpstorm/php-arrays.html#shape) 格式：
  ```php
  /** @return array{ok: bool, partition?: int, offset?: int} */
  ```
- `list<T>` 表示 0-indexed 列表
- 平行数组（如 `seek` 的三个数组）用 `string[]` / `int[]` 简单形式

## 实测

```bash
# 干净 PHP（不加载已装的扩展）语法验证
php -n -l stubs/hi_kafka.php
# → No syntax errors detected in ...
```

## 同步策略

stubs 与扩展实际 API 必须保持一致。规则：
- 每改 `ext/src/client.rs` 或 `ext/src/lib.rs` 增删 PHP 函数 / 方法
- 同步改 `stubs/hi_kafka.php`
- 加单元测试时一并改

CI 可加一条 stubs lint：

```yaml
- name: PHP stubs lint
  run: php -n -l hi-kafka-ext/stubs/hi_kafka.php
```
