<?php

declare(strict_types=1);

namespace DbalEs\Projection;

use DbalEs\EventStore;
use DbalEs\EventStreamId;

class EventStoreWithInlineProjectors implements EventStore
{
    /**
     * @param iterable<Projector> $projectors
     */
    public function __construct(
        private EventStore $eventStore,
        private array $projectors
    ) {
    }

    public function append(EventStreamId $eventStreamId, array $events): array
    {
        $events = $this->eventStore->append($eventStreamId, $events);

        foreach ($events as $event) {
            foreach ($this->projectors as $projector) {
                $projector->project($event);
            }
        }

        return $events;
    }

    public function load(EventStreamId $eventStreamId): iterable
    {
        return $this->eventStore->load($eventStreamId);
    }
}