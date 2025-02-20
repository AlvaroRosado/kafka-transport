<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Transport\Filter;
use RdKafka\Message;

interface RecordFilterStrategy
{
    /**
     * Return true if the record should be discarded.
     */
    public function filter(string $transportName, string $groupId, Message $message): bool;
}
