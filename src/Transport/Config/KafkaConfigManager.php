<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Transport\Config;

use Symfony\Component\Messenger\Exception\LogicException;

class KafkaConfigManager
{
    private const array DEFAULT_CONSUMER_OPTIONS = [
        'commit_async' => true,
        'consume_timeout_ms' => 500,
        'validate_schema' => false,
        'topics' => [],
        'config' => [
            'auto.offset.reset' => 'earliest',
            'enable.auto.commit' => 'false',
            'enable.partition.eof' => 'true',
        ],
    ];

    private const array DEFAULT_PRODUCER_OPTIONS = [
        'validate_schema' => false,
        'poll_timeout_ms' => 0,
        'flush_timeout_ms' => 10000,
        'topics' => [],
        'config' => [],
    ];


    private const array REQUIRED_CONSUMER_config = [
        'group.id',
    ];

    private const array REQUIRED_PRODUCER_config = [];

    public function setupConsumerOptions(array $configOptions, string $contextErrorMessage): array
    {
        $configOptions = $configOptions["consumer"];

        if (0 === \count($configOptions)) {
            return self::DEFAULT_CONSUMER_OPTIONS;
        }

        $invalidOptions = array_diff(
            array_keys($configOptions),
            array_keys(self::DEFAULT_CONSUMER_OPTIONS),
        );

        if (0 < \count($invalidOptions)) {
            throw new \InvalidArgumentException(sprintf('Invalid option(s) "%s" %s', implode('", "', $invalidOptions), $contextErrorMessage));
        }

        $options = array_replace_recursive(
            self::DEFAULT_CONSUMER_OPTIONS,
            $configOptions,
        );

        if (!\is_bool($options['commit_async'])) {
            throw new LogicException(sprintf('The "commit_async" option type must be boolean, "%s" %s', \gettype($options['commit_async']), $contextErrorMessage));
        }

        if (!\is_int($options['consume_timeout_ms'])) {
            throw new LogicException(sprintf('The "consume_timeout_ms" option type must be integer, "%s" %s.', \gettype($options['consume_timeout_ms']), $contextErrorMessage));
        }

        if (!\is_array($options['topics'])) {
            throw new LogicException(sprintf('The "topics" option type must be array, "%s" %s.', \gettype($options['topics']), $contextErrorMessage));
        }

        self::validateKafkaOptions($options['config'], KafkaConfigList::consumer(), $contextErrorMessage);

        if (self::REQUIRED_CONSUMER_config !== array_intersect(self::REQUIRED_CONSUMER_config, array_keys($options['config']))) {
            throw new LogicException(sprintf('The conf_option(s) "%s" are required %s.', implode('", "', self::REQUIRED_CONSUMER_config), $contextErrorMessage));
        }

        return $options;
    }

    public function setupProducerOptions(array $configOptions, string $contextErrorMessage): array
    {
        $configOptions = $configOptions["producer"];

        if (0 === \count($configOptions)) {
            return self::DEFAULT_PRODUCER_OPTIONS;
        }

        $invalidOptions = array_diff(
            array_keys($configOptions),
            array_keys(self::DEFAULT_PRODUCER_OPTIONS),
        );

        if (0 < \count($invalidOptions)) {
            throw new \InvalidArgumentException(sprintf('Invalid option(s) "%s" %s', implode('", "', $invalidOptions), $contextErrorMessage));
        }

        $options = array_replace_recursive(
            self::DEFAULT_PRODUCER_OPTIONS,
            $configOptions,
        );

        if (!\is_int($options['poll_timeout_ms'])) {
            throw new LogicException(sprintf('The "poll_timeout_ms" option type must be integer, "%s" %s', \gettype($options['poll_timeout_ms']), $contextErrorMessage));
        }

        if (!\is_int($options['flush_timeout_ms'])) {
            throw new LogicException(sprintf('The "flush_timeout_ms" option type must be integer, "%s" %s.', \gettype($options['flush_timeout_ms']), $contextErrorMessage));
        }

        if (!\is_array($options['topics'])) {
            throw new LogicException(sprintf('The "topics" option type must be array, "%s" %s.', \gettype($options['topics']), $contextErrorMessage));
        }

        self::validateKafkaOptions($options['config'], KafkaConfigList::producer(), $contextErrorMessage);

        if (self::REQUIRED_PRODUCER_config !== array_intersect_key(self::REQUIRED_PRODUCER_config, array_keys($options['config']))) {
            throw new LogicException(sprintf('The conf_option(s) "%s" are required for the %s.', implode('", "', self::REQUIRED_PRODUCER_config), $contextErrorMessage));
        }

        return $options;
    }

    private static function validateKafkaOptions(array $values, array $availableKafkaOptions, string $contextErrorMessage): void
    {
        foreach ($values as $key => $value) {
            if (!isset($availableKafkaOptions[$key])) {
                throw new \InvalidArgumentException(sprintf('Invalid config option "%s" %s', $key, $contextErrorMessage));
            }

            if (!\is_string($value)) {
                throw new LogicException(sprintf('Kafka config value "%s" must be a string, %s, %s', $key, get_debug_type($value), $contextErrorMessage));
            }
        }
    }

}
