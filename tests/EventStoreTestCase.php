<?php

declare(strict_types=1);

namespace DbalEsTests;

use DbalEs\Event;
use DbalEs\EventStore;
use DbalEs\EventStreamId;
use DbalEs\PersistedEvent;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Uid\Uuid;

trait EventStoreTestCase
{
    #[Test]
    public function it_can_persist_and_load_events(): void
    {
        $eventStore = $this->createEventStore();

        $eventStreamId = new EventStreamId(Uuid::v4()->toString());
        $eventStore->append($eventStreamId, [
            new Event('event_type', ['data' => 'value']),
            new Event('event_type', ['data' => 'value']),
        ]);
        $eventStore->append(new EventStreamId(Uuid::v4()->toString()), [
            new Event('event_type', ['data' => 'value']),
            new Event('event_type', ['data' => 'value']),
        ]);

        $events = \iterator_to_array($eventStore->load($eventStreamId));

        self::assertCount(2, $events);
        foreach ($events as $event) {
            self::assertInstanceOf(PersistedEvent::class, $event);
        }
    }
}