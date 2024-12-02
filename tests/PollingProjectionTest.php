<?php

declare(strict_types=1);

namespace DbalEsTests;


use DbalEs\Dbal\Pdo\PdoConnection;
use DbalEs\Event;
use DbalEs\EventStreamId;
use DbalEs\Postgres\PostgresEventStore;
use DbalEs\Postgres\PostgresPersistentSubscriptions;
use DbalEs\Projection\PollingProjection;
use DbalEs\Subscription\SubscriptionQuery;
use DbalEsTests\Fixtures\InMemoryEventCounterProjector;
use PHPUnit\Framework\Attributes\CoversNothing;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Uid\Uuid;

#[CoversNothing]
class PollingProjectionTest extends TestCase
{
    #[Test]
    public function polling_projection(): void
    {
        $pdo = new \PDO('pgsql:host=localhost;port=25432;dbname=app;user=app;password=!ChangeMe!');
        $connection = new PdoConnection($pdo);

        $eventStore = new PostgresEventStore($connection);
        $persistentSubscriptions = new PostgresPersistentSubscriptions($connection, $eventStore);

        $streamId = new EventStreamId(Uuid::v4());

        $projector = new InMemoryEventCounterProjector();
        $persistentSubscriptions->deleteSubscription(__METHOD__);
        $persistentSubscriptions->createSubscription(__METHOD__, new SubscriptionQuery(streamIds: [$streamId->streamId]));
        $projection = new PollingProjection(__METHOD__, $projector, $persistentSubscriptions);

        $projection->run();

        self::assertEquals(0, $projector->getCounter());

        $eventStore->append($streamId, [
            new Event('event_type', ['data' => 'value']),
            new Event('event_type', ['data' => 'value']),
        ]);

        $projection->run();

        self::assertEquals(2, $projector->getCounter());

        $eventStore->append($streamId, [
            new Event('event_type', ['data' => 'value']),
        ]);

        $projection->run();

        self::assertEquals(3, $projector->getCounter());
    }
}