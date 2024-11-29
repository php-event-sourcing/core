<?php

declare(strict_types=1);

namespace DbalEsTests\Fixtures;

use DbalEs\PersistedEvent;
use DbalEs\Projection\Projector;

class InMemoryEventCounterProjector implements Projector
{
    private int $counter = 0;
    public function project(PersistedEvent $event): void
    {
        $this->counter++;
    }

    public function getCounter(): int
    {
        return $this->counter;
    }
}