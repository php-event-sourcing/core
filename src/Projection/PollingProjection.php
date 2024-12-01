<?php

declare(strict_types=1);

namespace DbalEs\Projection;

use DbalEs\Subscription\SubscriptionLoader;
use DbalEs\Subscription\SubscriptionQuery;

class PollingProjection
{
    public function __construct(
        private string $name,
        private Projector $projector,
        private SubscriptionLoader $subscriptionLoader,
        private PollingProjectionManager $projectionManager,
        private SubscriptionQuery $subscriptionQuery = new SubscriptionQuery(),
    ) {
    }

    public function run(): int
    {
        $position = $this->projectionManager->lockState($this->name);
        $eventStream = $this->subscriptionLoader->read($this->subscriptionQuery->withPosition($position));

        $count = 0;
        foreach ($eventStream as $event) {
            $this->projector->project($event);
            $position = $event->eventId;
            $count++;
        }

        $this->projectionManager->releaseState($this->name, $position);
        return $count;
    }
}