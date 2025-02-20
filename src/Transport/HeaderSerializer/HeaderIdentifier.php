<?php

declare(strict_types=1);


namespace Exoticca\KafkaMessenger\Transport\HeaderSerializer;

class HeaderIdentifier {
    public const string SYMFONY_HEADER_PREFIX = 'X-Message-Stamp-';
    public const string HEADER_PREFIX = 'X-exoticca-';
}