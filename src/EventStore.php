<?php

declare(strict_types=1);

namespace DbalEs;

interface EventStore
{
    /**
     * @param Event[] $events
     * @return PersistedEvent[]
     */
    public function append(EventStreamId $eventStreamId, array $events): array;

    /**
     * @return iterable<PersistedEvent>
     */
    public function load(EventStreamId $eventStreamId): iterable;
}