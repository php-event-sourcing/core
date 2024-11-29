<?php

declare(strict_types=1);

namespace DbalEsTests\Postgres;

use DbalEs\Dbal\Pdo\PdoConnection;
use DbalEs\EventStore;
use DbalEs\Postgres\PostgresEventStore;
use DbalEsTests\EventStoreTestCase;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(PostgresEventStore::class)]
class PostgresEventStoreTest extends TestCase
{
    use EventStoreTestCase;
    protected function createEventStore(): EventStore
    {
        $pdo = new \PDO('pgsql:host=localhost;port=25432;dbname=app;user=app;password=!ChangeMe!');
        $connection = new PdoConnection($pdo);

        return new PostgresEventStore($connection);
    }
}
