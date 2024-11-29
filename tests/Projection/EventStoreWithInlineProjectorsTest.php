<?php

declare(strict_types=1);

namespace DbalEsTests\Projection;

use DbalEs\Event;
use DbalEs\EventStreamId;
use DbalEs\Projection\EventStoreWithInlineProjectors;
use DbalEs\Test\InMemoryEventStore;
use DbalEsTests\Fixtures\InMemoryEventCounterProjector;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Uid\Uuid;

#[CoversClass(EventStoreWithInlineProjectors::class)]
class EventStoreWithInlineProjectorsTest extends TestCase
{
    #[Test]
    public function it_appends_events_and_projects_them(): void
    {
        $eventStore = new InMemoryEventStore();
        $projector = new InMemoryEventCounterProjector();
        $eventStoreWithProjectors = new EventStoreWithInlineProjectors($eventStore, [$projector]);

        $eventStoreWithProjectors->append(new EventStreamId(Uuid::v4()), [
            new Event('event_type', ['data' => 'value']),
        ]);

        self::assertSame(1, $projector->getCounter());
    }

    #[Test]
    public function it_loads_events(): void
    {
        $eventStore = new InMemoryEventStore();
        $eventStoreWithProjectors = new EventStoreWithInlineProjectors($eventStore, []);

        $eventStreamId = new EventStreamId(Uuid::v4());
        $eventStore->append($eventStreamId, [
            new Event('event_type', ['data' => 'value']),
        ]);

        $events = \iterator_to_array($eventStoreWithProjectors->load($eventStreamId));

        self::assertCount(1, $events);
    }

}
