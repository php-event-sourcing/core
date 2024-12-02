<?php

declare(strict_types=1);

namespace DbalEs\Projection;

use DbalEs\Subscription\PersistentSubscriptions;

class PollingProjection
{
    public function __construct(
        private string                  $subscriptionName,
        private Projector               $projector,
        private PersistentSubscriptions $persistentSubscriptions,
    ) {
    }

    public function run(): int
    {
        $page = $this->persistentSubscriptions->read($this->subscriptionName);
        $count = 0;
        foreach ($page->events as $event) {
            $this->projector->project($event);
            $count++;
        }

        $this->persistentSubscriptions->ack($page);
        return $count;
    }
}