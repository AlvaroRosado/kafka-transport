<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Transport\Config;

final class KafkaProducerConfig
{
    private array $config;
    private array $topics;
    private int $pollTimeoutMs;
    private int $flushTimeoutMs;
    private bool $validateSchema;
    public function __construct(
        array $config,
        array $topics,
        int $pollTimeoutMs,
        int $flushTimeoutMs,
        bool $validateSchema,
    ) {
        $this->config = $config;
        $this->topics = $topics;
        $this->pollTimeoutMs = $pollTimeoutMs;
        $this->flushTimeoutMs = $flushTimeoutMs;
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

    public function pollTimeoutMs(): int
    {
        return $this->pollTimeoutMs;
    }

    public function flushTimeoutMs(): int
    {
        return $this->flushTimeoutMs;
    }

    public function validateSchema(): bool
    {
        return $this->validateSchema;
    }
}
