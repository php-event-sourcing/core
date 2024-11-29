<?php

declare(strict_types=1);

namespace DbalEs\Test;

use DbalEs\EventStore;
use DbalEs\EventStreamId;
use DbalEs\PersistedEvent;
use DbalEs\Subscription\SubscriptionLoader;
use DbalEs\Subscription\SubscriptionPosition;
use DbalEs\Subscription\SubscriptionQuery;

class InMemoryEventStore implements EventStore, SubscriptionLoader
{
    private int $nextEventId = 1;

    /** @var array<PersistedEvent>  */
    private array $events = [];

    /** @var array<InMemoryStream>  */
    private array $streams = [];

    public function append(EventStreamId $eventStreamId, array $events): array
    {
        $stream = $this->streams[(string) $eventStreamId->streamId] ?? null;
        if ($stream === null) {
            $stream = $this->streams[(string) $eventStreamId->streamId] = new InMemoryStream((string) $eventStreamId->streamId, 0);
        }
        $persistedEvents = [];
        foreach ($events as $event) {
            $persistedEvents[] = $persistedEvent = new PersistedEvent(
                new SubscriptionPosition(1, $this->nextEventId),
                $event,
                new EventStreamId($eventStreamId->streamId, $stream->version++),
            );
            $stream->events[] = $this->events[$this->nextEventId] = $persistedEvent;
            $this->nextEventId++;
        }

        return $persistedEvents;
    }

    public function load(EventStreamId $eventStreamId): iterable
    {
        $stream = $this->streams[(string) $eventStreamId->streamId] ?? null;
        if ($stream === null) {
            return [];
        }
        reset($stream->events);
        foreach ($stream->events as $event) {
            if ($eventStreamId->version && $event->eventStreamId->version < $eventStreamId->version) {
                continue;
            }
            yield $event;
        }
    }

    public function read(SubscriptionQuery $query): iterable
    {
        foreach ($this->events as $event) {
            if ($query->streamIds && !in_array($event->eventStreamId->streamId, $query->streamIds, true)) {
                continue;
            }
            if ($query->from && $event->eventId->transactionId < $query->from->transactionId) {
                continue;
            }
            if ($query->from && $event->eventId->transactionId === $query->from->transactionId && $event->eventId->sequenceNumber <= $query->from->sequenceNumber) {
                continue;
            }
            yield $event;
        }
    }
}

/**
 * @internal
 */
class InMemoryStream {
    /**
     * @param array<PersistedEvent> $events
     */
    public function __construct(
        public string $streamId,
        public int $version,
        public array $events = [],
    ) {
    }
}