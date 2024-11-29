<?php

declare(strict_types=1);

namespace DbalEs\Projection;

use DbalEs\Subscription\SubscriptionLoader;
use DbalEs\Subscription\SubscriptionQuery;

class PollingProjection
{
    public function __construct(
        private string $projectionName,
        private Projector $projector,
        private SubscriptionLoader $subscriptionLoader,
        private SubscriptionQuery $subscriptionQuery,
    ) {
    }

    public function run(ProjectionState $state): ProjectionState
    {
        $position = $state->position;
        $eventStream = $this->subscriptionLoader->read($this->subscriptionQuery->withPosition($position));

        foreach ($eventStream as $event) {
            $this->projector->project($event);
            $position = $event->eventId;
        }

        return $state->withPosition($position);
    }
}