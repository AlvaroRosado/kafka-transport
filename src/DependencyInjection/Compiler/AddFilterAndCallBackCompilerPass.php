<?php

declare(strict_types=1);


namespace Exoticca\KafkaMessenger\DependencyInjection\Compiler;

use Exoticca\KafkaMessenger\Transport\Callback\CallbackManager;
use Exoticca\KafkaMessenger\Transport\Callback\CallbackProcessorInterface;
use Exoticca\KafkaMessenger\Transport\Filter\RecordFilterManager;
use Exoticca\KafkaMessenger\Transport\Filter\RecordFilterStrategy;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Reference;

class AddFilterAndCallBackCompilerPass implements CompilerPassInterface {

    public function process(ContainerBuilder $container): void
    {
        $callBackManager = $container->findDefinition(CallbackManager::class);
        $recordFilterManager = $container->findDefinition(RecordFilterManager::class);

        foreach ($container->getDefinitions() as $serviceId => $definition) {
            if (is_subclass_of($definition->getClass(), CallbackProcessorInterface::class)) {
                $callBackManager->addMethodCall('addCallbackProcessor', [new Reference($serviceId)]);
            }
            if (is_subclass_of($definition->getClass(), RecordFilterStrategy::class)) {
                $recordFilterManager->addMethodCall('addFilter', [new Reference($serviceId)]);
            }
        }
    }
}