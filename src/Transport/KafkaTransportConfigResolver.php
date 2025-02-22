<?php

namespace Exoticca\KafkaMessenger\Transport;

use Exoticca\KafkaMessenger\Transport\Setting\ConsumerSetting;
use Exoticca\KafkaMessenger\Transport\Setting\GeneralSetting;
use Exoticca\KafkaMessenger\Transport\Setting\ProducerSetting;
use Exoticca\KafkaMessenger\Transport\Setting\SettingManager;
use LogicException;

final class KafkaTransportConfigResolver
{
    private const array AVAILABLE_OPTIONS = [
        'consumer',
        'producer',
        'topics',
        'validate_schema',
        'schema_registry',
        'transport_name',
        'serializer'
    ];

    public function resolve(string $dsn, array $globalOptions, array $transportOptions): GeneralSetting
    {
        if (false === $parsedUrl = parse_url($dsn)) {
            throw new \InvalidArgumentException(sprintf('The given Kafka DSN "%s" is invalid.', $dsn));
        }

        if ('kafka' !== $parsedUrl['scheme']) {
            throw new \InvalidArgumentException(sprintf('The given Kafka DSN "%s" must start with "kafka://".', $dsn));
        }

        $options = array_replace_recursive($globalOptions, $transportOptions);
        $transportName = $options["transport_name"];

        $invalidOptions = array_diff(
            array_keys($options),
            self::AVAILABLE_OPTIONS,
        );

        if (0 < \count($invalidOptions)) {
            throw new \InvalidArgumentException(sprintf('Invalid option(s) "%s" passed to the %s transport.', implode('", "', $invalidOptions), $transportName));
        }

        if (
            !\array_key_exists('consumer', $options)
            && !\array_key_exists('producer', $options)
        ) {
            throw new LogicException('At least one of "consumer" or "producer" options is required for the %s transport.'. $transportName);
        }

        $configValidator = new SettingManager();
        $consumerOptions = $configValidator->setupConsumerOptions($options, sprintf(" given in the consumer option of transport %s", $transportName));
        $producerOptions = $configValidator->setupProducerOptions($options, sprintf(" given in the producer option of transport %s", $transportName));

        if (empty($consumerOptions['topics']) && empty($producerOptions['topics']) && empty($options['topics'])) {
            throw new LogicException(sprintf('At least one of "consumer.topics", "producer.topics" or "topics" options is required for the %s transport.', $transportName));
        }

        $topics = $options['topics'] ?? null;

        if (empty($consumerOptions['topics'])) {
            $consumerOptions['topics'] = $topics;
        }

        if (empty($producerOptions['topics'])) {
            $producerOptions['topics'] = $topics;
        }

        $validateSchema = $options['validate_schema'] ?? null;

        if (!is_null($validateSchema)) {
            $consumerOptions['validate_schema'] = $validateSchema;
            $producerOptions['validate_schema'] = $validateSchema;
        }

        $consumerOptions['routing'] = array_column($consumerOptions['routing'], 'class', 'name');

        $consumerConfig = new ConsumerSetting(
            routing: $consumerOptions['routing'],
            config: $consumerOptions['config'],
            topics: $consumerOptions['topics'],
            consumeTimeout: $consumerOptions['consume_timeout_ms'],
            commitAsync: $consumerOptions['commit_async'],
            validateSchema: $consumerOptions['validate_schema'],
        );

        $producerConfig = new ProducerSetting(
            config: $producerOptions['config'],
            topics: $producerOptions['topics'],
            pollTimeoutMs: $producerOptions['poll_timeout_ms'],
            flushTimeoutMs: $producerOptions['flush_timeout_ms'],
            validateSchema: $producerOptions['validate_schema'],
        );

        return new GeneralSetting(
            host: $parsedUrl["host"].":".$parsedUrl["port"],
            transportName: $options["transport_name"],
            producer: $producerConfig,
            consumer: $consumerConfig,
        );
    }
}
