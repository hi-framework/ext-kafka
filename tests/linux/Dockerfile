# Linux 验证镜像：与生产一致的 Alpine 3.24 / musl / PHP 8.2 + Swoole，
# 加上 e2e 与 soak 需要的工具。扩展来自 Dockerfile.alpine 的 builder 阶段。
#
#   docker build -f Dockerfile.alpine --target builder -t hi-kafka-builder:ARCH .
#   docker build -f tests/linux/Dockerfile --build-arg BUILDER=hi-kafka-builder:ARCH \
#       -t hi-kafka-test:ARCH .
#
# 运行方式见 tests/linux/run.sh：源码以 volume 挂载，broker 走宿主网络，
# docker socket 挂载用于注入 broker 故障。

ARG BUILDER=localhost/hi-kafka-builder:amd64
ARG PHP_VERSION=8.2
ARG ALPINE_VARIANT=alpine3.24
# 与生产一致：Swoole 4.8（4.8.13 是该系列支持 PHP 8.2 的最终版本）。
ARG SWOOLE_VERSION=4.8.13
# FROM 中使用的 ARG 必须声明在第一个 FROM 之前（全局作用域）。
ARG RUNTIME_IMAGE=php:${PHP_VERSION}-cli-${ALPINE_VARIANT}

FROM ${BUILDER} AS builder

FROM ${RUNTIME_IMAGE}
ARG SWOOLE_VERSION
ARG APK_MIRROR=mirrors.ustc.edu.cn

RUN if [ -n "${APK_MIRROR}" ]; then \
        sed -i "s|dl-cdn.alpinelinux.org|${APK_MIRROR}|g" /etc/apk/repositories; \
    fi \
    # 与 Dockerfile.alpine runtime 阶段相同的运行时依赖
    && apk add --no-cache libsasl zlib libcurl libgcc libstdc++ ca-certificates \
    # 测试工具：完整 ps（busybox ps 不支持 -o/-p）、lsof、bash、docker CLI
    && apk add --no-cache bash procps lsof docker-cli \
    && apk add --no-cache --virtual .build-deps $PHPIZE_DEPS linux-headers \
    && (php -m | grep -q '^pcntl$' || docker-php-ext-install pcntl) \
    # 显式给出全部 configure 选项，不依赖交互默认值（默认开启的 brotli 需要额外依赖）
    && pecl install -D 'enable-sockets="no" enable-openssl="no" enable-mysqlnd="no" enable-swoole-curl="no" enable-cares="no" enable-brotli="no" enable-swoole-pgsql="no" with-swoole-odbc="no" with-swoole-oracle="no" enable-swoole-sqlite="no"' swoole-${SWOOLE_VERSION} \
    && apk del .build-deps \
    && rm -rf /tmp/pear

COPY --from=builder /src/target/release/libhi_kafka.so /opt/hi-kafka/libhi_kafka.so

WORKDIR /src
CMD ["php", "-v"]
