<?php

declare(strict_types=1);


namespace Exoticca\KafkaMessenger\Transport\Config;

interface KafkaConfigInterface
{
    public function host(): string;
    public function transportName(): string;
    public function producer(): KafkaProducerConfig;
    public function consumer(): KafkaConsumerConfig;
}