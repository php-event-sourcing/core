<?php

declare(strict_types=1);

namespace DbalEsTests\Postgres;

use DbalEs\Dbal\Pdo\PdoConnection;
use DbalEs\Event;
use DbalEs\EventStreamId;
use DbalEs\Postgres\PostgresEventStore;
use DbalEs\Postgres\PostgresPersistentSubscriptions;
use DbalEs\Subscription\SubscriptionQuery;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Uid\Uuid;

#[CoversClass(PostgresPersistentSubscriptions::class)]
class PostgresPersistentSubscriptionsTest extends TestCase
{
    #[Test]
    public function it_can_subscribe_to_a_stream(): void
    {
        $pdo = new \PDO('pgsql:host=localhost;port=25432;dbname=app;user=app;password=!ChangeMe!');
        $connection = new PdoConnection($pdo);
        $eventStore = new PostgresEventStore($connection);
        $persistentSubscriptions = new PostgresPersistentSubscriptions($connection, $eventStore);
        $eventStreamId = new EventStreamId(Uuid::v4()->toString());

        $persistentSubscriptions->deleteSubscription(__METHOD__);
        $persistentSubscriptions->createSubscription(__METHOD__, new SubscriptionQuery(streamIds: [$eventStreamId->streamId]));

        $page = $persistentSubscriptions->read(__METHOD__);
        self::assertEmpty($page->events);

        $eventStore->append($eventStreamId, [
            new Event('event_type', ['data' => 'value']),
            new Event('event_type', ['data' => 'value']),
        ]);

        $page = $persistentSubscriptions->read(__METHOD__);
        self::assertCount(2, $page->events);

        $eventStore->append($eventStreamId, [
            new Event('event_type', ['data' => 'value']),
            new Event('event_type', ['data' => 'value']),
        ]);

        $page = $persistentSubscriptions->read(__METHOD__);
        self::assertCount(4, $page->events);

        $persistentSubscriptions->ack($page);
        $page = $persistentSubscriptions->read(__METHOD__);
        self::assertCount(0, $page->events);
    }
}
