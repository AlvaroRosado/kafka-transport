<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Transport\Filter;

use RdKafka\Message;

class RecordFilterManager
{
    /** @var RecordFilterStrategy[] $filters */
    private array $filters;

    public function __construct()
    {
        $this->filters = [];
    }

    public function getFilters(): array
    {
        return $this->filters;
    }

    public function addFilter(RecordFilterStrategy $filter): void
    {
        $this->filters[] = $filter;
    }

    public function filter(string $transportName, string $groupId, Message $message): bool
    {
        foreach ($this->filters as $filter) {
            if ($filter->filter($transportName, $groupId, $message)) {
                return true;
            }
        }
        return false;
    }
}
