<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Transport;

use Exoticca\KafkaMessenger\SchemaRegistry\SchemaRegistryManager;
use Exoticca\KafkaMessenger\Transport\Callback\CallbackManager;
use Exoticca\KafkaMessenger\Transport\Filter\RecordFilterManager;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

final readonly class KafkaTransportFactory implements TransportFactoryInterface
{
    private KafkaTransportConfigResolver $configuration;
    private CallbackManager $callbackManager;
    private ?array $globalConfig;
    private RecordFilterManager $recordFilterManager;
    private ?SchemaRegistryManager $schemaRegistryManager;

    public function __construct(
        KafkaTransportConfigResolver $configuration,
        CallbackManager              $callbackManager,
        RecordFilterManager          $recordFilterManager,
        ?SchemaRegistryManager       $schemaRegistryManager = null,
        ?array                       $globalConfig = null,
    ) {
        $this->configuration = $configuration;
        $this->callbackManager = $callbackManager;
        $this->globalConfig = $globalConfig;
        $this->recordFilterManager = $recordFilterManager;
        $this->schemaRegistryManager = $schemaRegistryManager;
    }

    public function createTransport(string $dsn, array $options, SerializerInterface $serializer): TransportInterface
    {
        $options = $this->configuration->resolve($dsn, $this->globalConfig, $options);

        $connection = new KafkaConnection(
            callbackManager: $this->callbackManager,
            manager: $this->recordFilterManager,
            kafkaContext: $options,
        );

        return new KafkaTransport(
            sender: new KafkaTransportSender(
                connection: $connection,
                serializer: $serializer,
                schemaRegistryManager: $options->producer()->validateSchema() ? $this->schemaRegistryManager : null,
            ),
            receiver: new KafkaTransportReceiver(
                connection: $connection,
                serializer: $serializer,
                schemaRegistryManager: $options->consumer()->validateSchema() ? $this->schemaRegistryManager : null,
            )
        );
    }

    public function supports(string $dsn, array $options): bool
    {
        return str_starts_with($dsn, 'kafka://');
    }
}
