<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Transport\Stamp;

use Symfony\Component\Messenger\Stamp\StampInterface;

final class KafkaMessageIdentifier implements StampInterface
{
    public const IDENTIFIER = 'identifier';

    public function __construct(public string $identifier)
    {
    }

    public function __toString(): string
    {
        return $this->identifier;
    }

}
