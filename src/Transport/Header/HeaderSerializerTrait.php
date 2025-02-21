<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Transport\Header;

use Exoticca\KafkaMessenger\Transport\Stamp\KafkaCustomHeadersStamp;
use Exoticca\KafkaMessenger\Transport\Stamp\KafkaMessageIdentifier;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Stamp\NonSendableStampInterface;
use Symfony\Component\Messenger\Stamp\StampInterface;

trait HeaderSerializerTrait
{
    /**
     * @param array $kafkaHeaders
     * @return StampInterface[]
     */
    public function decodeHeaders(array $encodedEnvelope): array
    {
        $kafkaHeaders = $encodedEnvelope["headers"];
        $stamps = [];
        $customHeadersStamp = new KafkaCustomHeadersStamp();
        $customHeaderStamp = null;

        foreach ($kafkaHeaders as $name => $value) {
            if (str_starts_with($name, HeaderIdentifier::SYMFONY_HEADER_PREFIX)) {
                try {
                    $content = unserialize($value);
                    $stamps[$content::class] = $content;
                } catch (\Throwable $e) {
                    throw new MessageDecodingFailedException('Could not decode stamp: '.$e->getMessage(), $e->getCode(), $e);
                }
            }
            if (str_starts_with($name, HeaderIdentifier::HEADER_PREFIX.KafkaMessageIdentifier::IDENTIFIER)) {
                $stamps[KafkaMessageIdentifier::class] = new KafkaMessageIdentifier($value);
            }
            if (str_starts_with($name, HeaderIdentifier::HEADER_PREFIX)) {
                $customHeaderStamp = $customHeadersStamp->withHeader($name, $value);
            }
        }

        if ($customHeaderStamp) {
            $stamps[$customHeaderStamp::class] = $customHeaderStamp;
        }

        return $stamps;
    }


    public function encodeHeaders(Envelope $envelope): array
    {
        $envelope = $envelope->withoutAll(NonSendableStampInterface::class);
        $allStamps = $envelope->all();
        $headers = [];

        /**
         * @var class-string $class
         * @var StampInterface[] $stamps
         */
        foreach ($allStamps as $class => $stamps) {
            foreach ($stamps as $stamp) {
                if ($stamp instanceof KafkaCustomHeadersStamp) {
                    foreach ($stamp->getHeaders() as $key => $value) {
                        $headers[HeaderIdentifier::HEADER_PREFIX.$key] = $value;
                    }
                    continue;
                }
                if ($stamp instanceof KafkaMessageIdentifier) {
                    $headers[HeaderIdentifier::HEADER_PREFIX.KafkaMessageIdentifier::IDENTIFIER] = $stamp->identifier;
                    continue;
                }
                $headers[HeaderIdentifier::SYMFONY_HEADER_PREFIX.$class] = serialize($stamp);
            }
        }

        return $headers;
    }
}
