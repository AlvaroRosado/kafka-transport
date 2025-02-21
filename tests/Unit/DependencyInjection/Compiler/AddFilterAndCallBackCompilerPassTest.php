<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Tests\Unit\DependencyInjection\Compiler;

use Exoticca\KafkaMessenger\DependencyInjection\Compiler\AddFilterAndCallBackCompilerPass;
use Exoticca\KafkaMessenger\Transport\Callback\CallbackManager;
use Exoticca\KafkaMessenger\Transport\Callback\CallbackProcessorInterface;
use Exoticca\KafkaMessenger\Transport\Filter\RecordFilterManager;
use Exoticca\KafkaMessenger\Transport\Filter\RecordFilterStrategy;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use RdKafka\KafkaConsumer;
use RdKafka\Message;
use RdKafka\Producer;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Reference;

#[CoversClass(AddFilterAndCallBackCompilerPass::class)]
class AddFilterAndCallBackCompilerPassTest extends TestCase
{
    public function testProcessAddsCallbackProcessorsAndFilters(): void
    {
        $container = new ContainerBuilder();

        $callbackManagerDefinition = new Definition(CallbackManager::class);
        $recordFilterManagerDefinition = new Definition(RecordFilterManager::class);

        $container->setDefinition(CallbackManager::class, $callbackManagerDefinition);
        $container->setDefinition(RecordFilterManager::class, $recordFilterManagerDefinition);

        $callbackProcessorDefinition = new Definition(MockCallbackProcessor::class);
        $recordFilterDefinition = new Definition(MockRecordFilter::class);

        $container->setDefinition('callback.processor.service', $callbackProcessorDefinition);
        $container->setDefinition('record.filter.service', $recordFilterDefinition);

        (new AddFilterAndCallBackCompilerPass())->process($container);

        $this->assertTrue($callbackManagerDefinition->hasMethodCall('addCallbackProcessor'));
        $this->assertTrue($recordFilterManagerDefinition->hasMethodCall('addFilter'));

        $this->assertEquals(
            new Reference('callback.processor.service'),
            $callbackManagerDefinition->getMethodCalls()[0][1][0]
        );

        $this->assertEquals(
            new Reference('record.filter.service'),
            $recordFilterManagerDefinition->getMethodCalls()[0][1][0]
        );
    }
}

class MockCallbackProcessor implements CallbackProcessorInterface
{
    public function log(object $kafka, int $level, string $facility, string $message): void
    {
        // TODO: Implement log() method.
    }

    public function consumerError(KafkaConsumer $kafka, int $err, string $reason): void
    {
        // TODO: Implement consumerError() method.
    }

    public function producerError(Producer $kafka, int $err, string $reason): void
    {
        // TODO: Implement producerError() method.
    }

    public function stats(object $kafka, string $json, int $jsonLength): void
    {
        // TODO: Implement stats() method.
    }

    public function rebalance(KafkaConsumer $kafka, int $err, array $partitions): void
    {
        // TODO: Implement rebalance() method.
    }

    public function consume(Message $message): void
    {
        // TODO: Implement consume() method.
    }

    public function offsetCommit(object $kafka, int $err, array $partitions): void
    {
        // TODO: Implement offsetCommit() method.
    }

    public function deliveryReport(object $kafka, Message $message): void
    {
        // TODO: Implement deliveryReport() method.
    }
}
class MockRecordFilter implements RecordFilterStrategy
{
    public function filter(string $transportName, string $groupId, Message $message): bool
    {
        // TODO: Implement filter() method.
    }
}
