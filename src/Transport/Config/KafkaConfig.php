<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Transport\Config;

final class KafkaConfig implements KafkaConfigInterface
{
    private string $host;
    private string $transportName;
    private KafkaProducerConfig $producerConfig;
    private KafkaConsumerConfig $consumerConfig;

    public function __construct(
        string $host,
        string $transportName,
        KafkaProducerConfig $producerConfig,
        KafkaConsumerConfig $consumerConfig,
    ) {
        $this->host = $host;
        $this->transportName = $transportName;
        $this->producerConfig = $producerConfig;
        $this->consumerConfig = $consumerConfig;
    }

    public function host(): string
    {
        return $this->host;
    }

    public function transportName(): string
    {
        return $this->transportName;
    }

    public function producer(): KafkaProducerConfig
    {
        return $this->producerConfig;
    }

    public function consumer(): KafkaConsumerConfig
    {
        return $this->consumerConfig;
    }
}
