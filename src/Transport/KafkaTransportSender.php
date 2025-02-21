<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Transport;

use Exception;
use Exoticca\KafkaMessenger\SchemaRegistry\SchemaRegistryManager;
use Exoticca\KafkaMessenger\Transport\Header\HeaderIdentifier;
use Exoticca\KafkaMessenger\Transport\Stamp\KafkaForceFlushStamp;
use Exoticca\KafkaMessenger\Transport\Stamp\KafkaMessageIdentifier;
use Exoticca\KafkaMessenger\Transport\Stamp\KafkaNoFlushStamp;
use Exoticca\KafkaMessenger\Transport\Stamp\KafkaMessageStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

final class KafkaTransportSender implements SenderInterface
{
    public function __construct(
        private KafkaConnection      $connection,
        private ?SerializerInterface $serializer = new PhpSerializer(),
        private ?SchemaRegistryManager $schemaRegistryManager = null,
    ) {
    }

    public function send(Envelope $envelope): Envelope
    {
        $decodedEnvelope = $this->serializer->encode($envelope);

        if ($this->schemaRegistryManager && $this->serializer instanceof PhpSerializer) {
            throw new TransportException('Schema registry is enabled but the defined serializer is not compatible with it. You must use a different serializer.');
        }

        $key = null;
        $partition = \RD_KAFKA_PARTITION_UA;
        $messageFlags = \RD_KAFKA_CONF_OK;

        if ($messageStamp = $envelope->last(KafkaMessageStamp::class)) {
            $key = $messageStamp->key;
            $partition = $messageStamp->partition;
            $messageFlags = $messageStamp->messageFlags;
        }

        $forceFlush = true;

        if ($envelope->last(KafkaNoFlushStamp::class)) {
            $forceFlush = false;
        }

        if ($envelope->last(KafkaNoFlushStamp::class) && $envelope->last(KafkaForceFlushStamp::class)) {
            $forceFlush = true;
        }

        if (isset($decodedEnvelope["headers"]) && $decodedEnvelope["headers"][HeaderIdentifier::HEADER_PREFIX.KafkaMessageIdentifier::IDENTIFIER]) {
            $discriminatoryName = $decodedEnvelope["headers"][HeaderIdentifier::HEADER_PREFIX.KafkaMessageIdentifier::IDENTIFIER];
        } else {
            $discriminatoryName = null;
        }

        try {
            $this->connection->produce(
                partition: $partition,
                messageFlags: $messageFlags,
                body: $decodedEnvelope["body"],
                key: $key,
                headers: $decodedEnvelope["headers"] ?? [],
                forceFlush: $forceFlush,
                beforeProduceConvertBody: fn (string $topic, string $body) => $this->encodeWithSchemaRegistry($topic, $body, $discriminatoryName)
            );
        } catch (Exception $e) {
            throw new TransportException($e->getMessage(), 0, $e);
        }

        return $envelope;
    }

    public function encodeWithSchemaRegistry(
        string $topic,
        string $body,
        ?string $typeName = null
    ): string {
        if ($this->schemaRegistryManager) {
            $body = $this->schemaRegistryManager->encode(json_decode($body, true), $topic, $typeName);
        }
        return $body;
    }

}
