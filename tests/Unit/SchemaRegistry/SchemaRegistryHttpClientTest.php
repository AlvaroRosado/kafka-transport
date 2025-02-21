<?php

declare(strict_types=1);

namespace Exoticca\KafkaMessenger\Tests\Unit\SchemaRegistry;

use Avro\SchemaRegistry\ClientError;
use Avro\SchemaRegistry\Model\Error;
use Exoticca\KafkaMessenger\SchemaRegistry\Avro\AvroSubject;
use Exoticca\KafkaMessenger\SchemaRegistry\SchemaRegistryHttpClient;
use Exoticca\KafkaMessenger\Tests\ObjectMother\AvroSchemaMother;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\ParameterBag\ParameterBag;
use Symfony\Component\HttpClient\MockHttpClient;
use Symfony\Component\HttpClient\Response\MockResponse;

#[CoversClass(SchemaRegistryHttpClient::class)]
class SchemaRegistryHttpClientTest extends TestCase
{
    private SchemaRegistryHttpClient $client;
    private ParameterBag $parameterBag;

    protected function setUp(): void
    {
        $this->parameterBag = new ParameterBag([
            'app.schema_registry' => [
                'base_uri' => 'http://schema-registry.local',
                'api_key' => 'test_key',
                'api_secret' => 'test_secret',
            ],
        ]);
    }

    public function testGetRegisteredSchemaId(): void
    {
        $response = new MockResponse(json_encode(['id' => 100091]));
        $this->client = new SchemaRegistryHttpClient($this->parameterBag, new MockHttpClient($response));
        $schemaId = 100091;

        $subject = 'public_revenue_rule_fct-value';
        $schema = '{}';

        $id = $this->client->getRegisteredSchemaId($subject, $schema);

        $this->assertEquals($schemaId, $id);
    }

    public function testRegisterSchema(): void
    {
        $schemaId = 20001;
        $responseBody = json_encode(['id' => $schemaId]);
        $response = new MockResponse($responseBody);
        $this->client = new SchemaRegistryHttpClient($this->parameterBag, new MockHttpClient($response));

        $subject = 'new_subject';
        $schema = '{"type": "record", "name": "test"}';

        $id = $this->client->registerSchema($subject, $schema);

        $this->assertEquals($schemaId, $id);
    }

    public function testGetSchema(): void
    {
        $schemaId = 100091;
        $schema = '{"type": "record", "name": "test"}';
        $responseBody = json_encode(['schema' => $schema]);
        $response = new MockResponse($responseBody);
        $this->client = new SchemaRegistryHttpClient($this->parameterBag, new MockHttpClient($response));

        $retrievedSchema = $this->client->getSchema($schemaId);

        $this->assertEquals($schema, $retrievedSchema);
    }

    public function testGetSubjectSchema(): void
    {
        $response = json_decode(\Safe\file_get_contents(__DIR__.'/../../Fixtures/schema_union.json'), true);
        $response["schema"] = json_encode($response["schema"]);
        $response = new MockResponse(json_encode($response));
        $this->client = new SchemaRegistryHttpClient($this->parameterBag, new MockHttpClient($response));

        $avroSubject = AvroSubject::ofValue('subject');
        $avroSchema = $this->client->getSubjectSchema($avroSubject);
        $this->assertEquals($avroSchema, AvroSchemaMother::unionType());
    }

    public function testGetRegisteredSchemaIdWithNotFound(): void
    {
        $responseBody = json_encode([
            'error_code' => Error::SUBJECT_NOT_FOUND,
            'message' => 'Subject not found',
        ]);

        $response = new MockResponse($responseBody);
        $this->client = new SchemaRegistryHttpClient($this->parameterBag, new MockHttpClient($response));

        $subject = 'non_existent_subject';
        $schema = '{}';

        $id = $this->client->getRegisteredSchemaId($subject, $schema);
        $this->assertNull($id);
    }

    public function testJsonRequestWithInvalidJsonResponse(): void
    {
        $this->expectException(ClientError::class);
        $response = new MockResponse('invalid_json');
        $this->client = new SchemaRegistryHttpClient($this->parameterBag, new MockHttpClient($response));
        $this->client->getSchema(100091);
    }
}
