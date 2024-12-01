<?php

declare(strict_types=1);

namespace DbalEsTests\Fixtures;

use DbalEs\PersistedEvent;
use DbalEs\Projection\Projector;

class InMemoryEventCounterProjector implements Projector
{
    private int $counter = 0;

    public function __construct(
        private ?array $streams = null,
    ) {
        if ($streams) {
            $this->streams = array_map('strval', $streams);
        }
    }

    public function project(PersistedEvent $event): void
    {
        if ($this->streams && !in_array((string) $event->eventStreamId->streamId, $this->streams, true)) {
            return;
        }
        $this->counter++;
    }

    public function getCounter(): int
    {
        return $this->counter;
    }
}