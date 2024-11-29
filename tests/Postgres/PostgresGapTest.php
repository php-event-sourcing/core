<?php

declare(strict_types=1);

namespace DbalEsTests\Postgres;

use DbalEs\Dbal\Pdo\PdoConnection;
use DbalEs\Event;
use DbalEs\EventStreamId;
use DbalEs\Postgres\PostgresEventStore;
use DbalEs\Postgres\PostgresSubscriptionLoader;
use DbalEs\Subscription\SubscriptionQuery;
use PHPUnit\Framework\Attributes\CoversNothing;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Uid\Uuid;

#[CoversNothing]
class PostgresGapTest extends TestCase
{
    #[Test]
    public function it_handles_gaps_in_event_stream(): void
    {
        $session1 = new \PDO('pgsql:host=localhost;port=25432;dbname=app;user=app;password=!ChangeMe!');
        $session2 = new \PDO('pgsql:host=localhost;port=25432;dbname=app;user=app;password=!ChangeMe!');

        $connection1 = new PdoConnection($session1);
        $connection2 = new PdoConnection($session2);

        $eventStore1 = new PostgresEventStore($connection1);
        $eventStore2 = new PostgresEventStore($connection2);

        // Session 1
        {
            $session1->beginTransaction();
            $eventStreamId1 = new EventStreamId(Uuid::v4()->toString());
            $eventStore1->append($eventStreamId1, [
                    new Event('event_type', ['data' => 'value']),
                    new Event('event_type', ['data' => 'value']),
                ]);
        }

        // Session 2 must not see events from Session 1
        {
            // When I add and commit new events to a different stream
            $eventStore2->append($eventStreamId2 = new EventStreamId(Uuid::v4()->toString()), [
                new Event('event_type', ['data' => 'value']),
            ]);
            self::assertCount(
                0,
                iterator_to_array($eventStore2->read(new SubscriptionQuery(streamIds: [$eventStreamId1->streamId, $eventStreamId2->streamId]))),
                "Session 2 must not see neither events from Session 1 and Session 2 if no Gaps",
            );

            self::assertCount(
                1,
                iterator_to_array($eventStore2->read(new SubscriptionQuery(streamIds: [$eventStreamId1->streamId, $eventStreamId2->streamId], allowGaps: true))),
                "If gaps are allowed, Session 2 must not see events from Session 1 (not committed) but see events of Session 2",
            );

        }

        // Commit Session 1
        {
            try {
                $session1->commit();
            } catch (\Throwable $e) {
                $session1->rollBack();
                throw $e;
            }
        }

        self::assertCount(
            3,
            iterator_to_array($eventStore2->read(new SubscriptionQuery(streamIds: [$eventStreamId1->streamId, $eventStreamId2->streamId]))),
            "Session 2 must see events from Session 1 and Session 2 after Session 1 commits"
        );
    }

}