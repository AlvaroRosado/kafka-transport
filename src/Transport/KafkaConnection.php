<?php

declare(strict_types=1);


namespace Exoticca\KafkaMessenger\Transport;

use Exoticca\KafkaMessenger\Transport\Callback\CallbackManager;
use Exoticca\KafkaMessenger\Transport\Config\KafkaConfig;
use Exoticca\KafkaMessenger\Transport\Filter\RecordFilterManager;
use LogicException;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use RdKafka\Message;
use RdKafka\Producer;
use Symfony\Component\Console\SignalRegistry\SignalRegistry;
use Symfony\Component\Messenger\Exception\TransportException;

class KafkaConnection {

    private bool $consumerSubscribed = false;
    private bool $consumerMustBeRunning = false;
    private ?KafkaConsumer $consumer;
    private ?Producer $producer;

    private SignalRegistry $signalRegistry;

    public function __construct(
        private readonly CallbackManager $callbackManager,
        private readonly RecordFilterManager $manager,
        private readonly KafkaConfig     $kafkaContext,
    ) {
        $this->signalRegistry = new SignalRegistry();
        if (!\extension_loaded('rdkafka')) {
            throw new LogicException(sprintf('You cannot use the "%s" as the "rdkafka" extension is not installed.', __CLASS__));
        }
    }


    public function get(
        array $topicsToFilter,
    ): ?iterable
    {
        $consumer = $this->getConsumer();

        if (!$this->consumerSubscribed) {
            $consumer->subscribe(!empty($topicsToFilter) ? $topicsToFilter : $this->kafkaContext->consumer()->topics());
        }
        $this->consumerMustBeRunning = true;
        yield from $this->doReceive(
            timeout: $this->kafkaContext->consumer()->consumeTimeout() ?? 500
        );
    }

    private function doReceive(
        int $timeout,
    ): iterable
    {
        $this->signalRegistry->register(SIGINT, fn () => $this->consumerMustBeRunning = false);
        $this->signalRegistry->register(SIGTERM, fn () => $this->consumerMustBeRunning = false);

        while($this->consumerMustBeRunning) {
            $kafkaMessage = $this->getConsumer()->consume($timeout);

            if (null === $kafkaMessage) {
                yield null;
            }

            switch ($kafkaMessage->err) {
                case \RD_KAFKA_RESP_ERR_NO_ERROR:
                    $filterMessage = $this->manager->filter(
                        transportName: $this->kafkaContext->transportName(),
                        groupId: $this->kafkaContext->consumer()->config()['group.id'],
                        message: $kafkaMessage,
                    );

                    if ($filterMessage) {
                        $this->ack($kafkaMessage);
                        break;
                    }
                    yield $kafkaMessage;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                case RD_KAFKA_RESP_ERR__TRANSPORT:
                case RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART:
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    yield null;
                default:
                    throw new \LogicException($kafkaMessage->errstr(), $kafkaMessage->err);
                    break;
            }
        }
    }


    public function ack(Message $message): void
    {
        $consumer = $this->getConsumer();

        if ($this->kafkaContext->consumer()->consumeTimeout()) {
            $consumer->commitAsync($message);
        } else {
            $consumer->commit($message);
        }
    }

    /**
     * @param array<string, string> $headers
     * @throws \RdKafka\Exception
     */
    public function produce(
        int      $partition,
        int      $messageFlags,
        string   $body,
        string   $key = null,
        array    $headers = [],
        bool     $forceFlush = true,
        callable $beforeProduceConvertBody = null,
    ): void
    {
        $producer = $this->getProducer();

        foreach ($this->kafkaContext->producer()->topics() as $topic) {
            if ($beforeProduceConvertBody) {
                $body = $beforeProduceConvertBody($topic, $body);
            }

            $topic = $producer->newTopic($topic);
            $topic->producev(
                $partition,
                $messageFlags,
                $body,
                $key,
                $headers,
            );

            $producer->poll($this->kafkaContext->producer()->flushTimeoutMs());

            if ($forceFlush) {
            	$this->flush();
            }
        }
    }

    public function flush(): void
    {
        for ($flushRetries = 0; $flushRetries < 10; ++$flushRetries) {
            $result = $this->getProducer()->flush($this->kafkaContext->producer()->flushTimeoutMs());

            if (RD_KAFKA_RESP_ERR_NO_ERROR === $result) {
                break;
            }
        }

        if (RD_KAFKA_RESP_ERR_NO_ERROR !== $result) {
            throw new TransportException('Was unable to flush, messages might be lost!: '.$result, $result);
        }
    }

    private function getConsumer(): KafkaConsumer
    {
        return $this->consumer ??= $this->createConsumer($this->kafkaContext->consumer()->config());
    }

    private function getProducer(): Producer
    {
        return $this->producer ??= $this->createProducer($this->kafkaContext->producer()->config());
    }

    private function getBaseConf(): Conf
    {
        $conf = new Conf();
        $conf->set('metadata.broker.list', $this->kafkaContext->host());
        $conf->setLogCb([$this->callbackManager, 'log']);
        $conf->setStatsCb([$this->callbackManager, 'stats']);
        return $conf;
    }

    private function createConsumer(array $kafkaConfig): KafkaConsumer
    {
        $conf = $this->getBaseConf();
        $conf->setErrorCb([$this->callbackManager, 'consumerError']);
        $conf->setRebalanceCb([$this->callbackManager, 'rebalance']);
        $conf->setConsumeCb([$this->callbackManager, 'consume']);
        $conf->setOffsetCommitCb([$this->callbackManager, 'offsetCommit']);

        foreach ($kafkaConfig as $key => $value) {
            $conf->set($key, $value);
        }

        return new KafkaConsumer($conf);
    }

    /**
     * @param array<string, string> $kafkaConfig
     */
    private function createProducer(array $kafkaConfig): Producer
    {
        $conf = $this->getBaseConf();
        $conf->setErrorCb([$this->callbackManager, 'producerError']);
        $conf->setDrMsgCb([$this->callbackManager, 'deliveryReport']);

        foreach ($kafkaConfig as $key => $value) {
            $conf->set($key, $value);
        }

        return new Producer($conf);
    }
}