<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Transport\Config;

final class KafkaConsumerConfig
{
    private array $config;
    private array $topics;
    private int $consumeTimeout;
    private bool $commitAsync;
    private bool $validateSchema;

    public function __construct(
        array $config,
        array $topics,
        int $consumeTimeout,
        bool $commitAsync,
        bool $validateSchema,
    ) {
        $this->config = $config;
        $this->topics = $topics;
        $this->consumeTimeout = $consumeTimeout;
        $this->commitAsync = $commitAsync;
        $this->validateSchema = $validateSchema;
    }

    public function config(): array
    {
        return $this->config;
    }

    public function topics(): array
    {
        return $this->topics;
    }

    public function consumeTimeout(): int
    {
        return $this->consumeTimeout;
    }

    public function commitAsync(): bool
    {
        return $this->commitAsync;
    }

    public function validateSchema(): bool
    {
        return $this->validateSchema;
    }
}
